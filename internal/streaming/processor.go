package streaming

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/example/kafka-sorting-pipeline/internal/data"
	"github.com/example/kafka-sorting-pipeline/internal/kafkautil"
	"github.com/example/kafka-sorting-pipeline/internal/metrics"
	"github.com/example/kafka-sorting-pipeline/internal/sorter"
)

// StreamProcessor handles real-time processing of Kafka messages with partition awareness
type StreamProcessor struct {
	broker      string
	sourceTopic string
	idTopic     string
	nameTopic   string
	contTopic   string
	groupID     string
	chunkSize   int
	workDir     string
	metrics     *metrics.Metrics
	concurrency int
}

// NewStreamProcessor creates a new stream processor
func NewStreamProcessor(
	broker, sourceTopic, idTopic, nameTopic, contTopic, groupID string,
	chunkSize int, workDir string, concurrency int,
	metrics *metrics.Metrics) *StreamProcessor {
	return &StreamProcessor{
		broker:      broker,
		sourceTopic: sourceTopic,
		idTopic:     idTopic,
		nameTopic:   nameTopic,
		contTopic:   contTopic,
		groupID:     groupID,
		chunkSize:   chunkSize,
		workDir:     workDir,
		metrics:     metrics,
		concurrency: concurrency,
	}
}

// ProcessStream processes messages in real-time from Kafka with partition awareness
func (sp *StreamProcessor) ProcessStream(ctx context.Context) error {
	log.Printf("Starting stream processing with concurrency level %d", sp.concurrency)

	// Get topic metadata to determine partition count
	var partitions []kafka.Partition

	// Retry getting partitions in case topics aren't created yet
	for i := 0; i < 10; i++ {
		conn, err := kafka.Dial("tcp", sp.broker)
		if err != nil {
			log.Printf("Failed to connect to Kafka (attempt %d/%d): %v", i+1, 10, err)
			if i == 9 {
				return fmt.Errorf("failed to connect to Kafka after 10 attempts: %w", err)
			}
			time.Sleep(2 * time.Second)
			continue
		}

		partitions, err = conn.ReadPartitions(sp.sourceTopic)
		conn.Close()

		if err != nil {
			log.Printf("Failed to read partitions (attempt %d/%d): %v", i+1, 10, err)
			if i == 9 {
				return fmt.Errorf("failed to read partitions after 10 attempts: %w", err)
			}
			time.Sleep(2 * time.Second)
			continue
		}

		break
	}

	log.Printf("Found %d partitions for topic %s", len(partitions), sp.sourceTopic)

	// Process each partition concurrently
	var wg sync.WaitGroup
	errChan := make(chan error, len(partitions))

	// Limit concurrent processors to the configured concurrency level
	semaphore := make(chan struct{}, sp.concurrency)

	// Context for managing graceful shutdown
	processCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, partition := range partitions {
		wg.Add(1)
		go func(partitionID int) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := sp.processPartition(processCtx, partitionID); err != nil {
				log.Printf("Error processing partition %d: %v", partitionID, err)
				// Cancel all other processors on error
				cancel()
				errChan <- err
			}
		}(partition.ID)

		// Small delay to avoid overwhelming the system
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for all goroutines to complete or context cancellation
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		log.Println("Received shutdown signal, stopping all processors...")
		cancel()
		// Give processors time to finish gracefully
		<-done
		return ctx.Err()
	case <-done:
		// Check for errors
		close(errChan)
		for err := range errChan {
			if err != nil {
				return fmt.Errorf("partition processing error: %w", err)
			}
		}
		return nil
	}
}

// processPartition processes messages from a specific partition
func (sp *StreamProcessor) processPartition(ctx context.Context, partitionID int) error {
	log.Printf("Starting processor for partition %d", partitionID)

	// Create reader for specific partition
	reader := kafkautil.NewPartitionedReader(sp.broker, sp.sourceTopic, partitionID, 0)
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Warning: error closing reader for partition %d: %v", partitionID, err)
		}
	}()

	// Buffers for each sort key
	idBuffer := make([]data.Record, 0, sp.chunkSize)
	nameBuffer := make([]data.Record, 0, sp.chunkSize)
	contBuffer := make([]data.Record, 0, sp.chunkSize)

	// Process messages continuously
	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping processor for partition %d", partitionID)
			// Process any remaining records in buffers before shutting down
			if len(idBuffer) > 0 || len(nameBuffer) > 0 || len(contBuffer) > 0 {
				log.Printf("Processing remaining %d records in buffer for partition %d", len(idBuffer), partitionID)
				if err := sp.processBuffers(ctx, partitionID, idBuffer, nameBuffer, contBuffer); err != nil {
					log.Printf("Warning: error processing final buffers for partition %d: %v", partitionID, err)
				}
			}
			return ctx.Err()
		default:
			// Read message with timeout
			ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
			msg, err := reader.ReadMessage(ctxWithTimeout)
			cancel()

			if err != nil {
				// Handle timeout/context cancellation
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// Continue on other errors (might be temporary)
				sp.metrics.IncrementConsumptionErrors()
				log.Printf("Warning: error reading message from partition %d: %v", partitionID, err)
				continue
			}

			// Parse the record
			record, err := data.ParseCSV(string(msg.Value))
			if err != nil {
				sp.metrics.IncrementConsumptionErrors()
				log.Printf("Warning: error parsing message from partition %d: %v", partitionID, err)
				continue
			}

			// Add to buffers
			idBuffer = append(idBuffer, record)
			nameBuffer = append(nameBuffer, record)
			contBuffer = append(contBuffer, record)
			sp.metrics.IncrementRecordsConsumed()

			// Check if buffers are full and trigger sorting
			if len(idBuffer) >= sp.chunkSize {
				if err := sp.processBuffers(ctx, partitionID, idBuffer, nameBuffer, contBuffer); err != nil {
					return fmt.Errorf("failed to process buffers for partition %d: %w", partitionID, err)
				}

				// Reset buffers
				idBuffer = idBuffer[:0]
				nameBuffer = nameBuffer[:0]
				contBuffer = contBuffer[:0]
			}
		}
	}
}

// processBuffers sorts and produces the buffered records
func (sp *StreamProcessor) processBuffers(ctx context.Context, partitionID int, idBuffer, nameBuffer, contBuffer []data.Record) error {
	log.Printf("Processing buffers for partition %d with %d records each", partitionID, len(idBuffer))

	// Process each sort key concurrently
	var wg sync.WaitGroup
	errChan := make(chan error, 3)

	// Context for this specific buffer processing
	bufferCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Sort and produce by ID
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := sp.sortAndProduce(bufferCtx, idBuffer, sp.idTopic, sorter.SortByID, partitionID); err != nil {
			log.Printf("Error sorting by ID for partition %d: %v", partitionID, err)
			cancel() // Cancel other operations on error
			errChan <- fmt.Errorf("sort by ID: %w", err)
		}
	}()

	// Sort and produce by Name
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := sp.sortAndProduce(bufferCtx, nameBuffer, sp.nameTopic, sorter.SortByName, partitionID); err != nil {
			log.Printf("Error sorting by Name for partition %d: %v", partitionID, err)
			cancel() // Cancel other operations on error
			errChan <- fmt.Errorf("sort by Name: %w", err)
		}
	}()

	// Sort and produce by Continent
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := sp.sortAndProduce(bufferCtx, contBuffer, sp.contTopic, sorter.SortByContinent, partitionID); err != nil {
			log.Printf("Error sorting by Continent for partition %d: %v", partitionID, err)
			cancel() // Cancel other operations on error
			errChan <- fmt.Errorf("sort by Continent: %w", err)
		}
	}()

	// Wait for all goroutines to complete
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		// Check for errors
		close(errChan)
		for err := range errChan {
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// sortAndProduce sorts the records and produces them to the target topic
func (sp *StreamProcessor) sortAndProduce(ctx context.Context, records []data.Record, topic string, key sorter.SortKey, partitionID int) error {
	if len(records) == 0 {
		return nil
	}

	log.Printf("Sorting %d records by %s for partition %d", len(records), key, partitionID)

	// Sort the records
	switch key {
	case sorter.SortByID:
		sorter.SortRecordsByID(records)
	case sorter.SortByName:
		sorter.SortRecordsByName(records)
	case sorter.SortByContinent:
		sorter.SortRecordsByContinent(records)
	}

	// Create writer for target topic
	writer := kafkautil.NewPartitionedWriter(sp.broker, topic, 1)
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("Warning: error closing writer for topic %s: %v", topic, err)
		}
	}()

	// Produce sorted records
	const batchSize = 1000
	msgs := make([]kafka.Message, 0, batchSize)

	for i, record := range records {
		// Check for context cancellation periodically
		if i%100 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		msgs = append(msgs, kafka.Message{
			Key:   []byte(fmt.Sprintf("%d-%d", partitionID, i)),
			Value: []byte(record.ToCSV()),
		})

		// Send batch when full or at the end
		if len(msgs) >= batchSize || i == len(records)-1 {
			if err := sp.retryWithBackoff(ctx, func() error {
				return writer.WriteMessages(ctx, msgs...)
			}, 3); err != nil {
				sp.metrics.IncrementProductionErrors()
				return fmt.Errorf("failed to produce messages: %w", err)
			}

			sp.metrics.IncrementRecordsProduced(len(msgs))
			msgs = msgs[:0] // Reset batch
		}
	}

	log.Printf("Successfully produced %d sorted records to topic %s for partition %d", len(records), topic, partitionID)
	return nil
}

// retryWithBackoff implements exponential backoff retry mechanism
func (sp *StreamProcessor) retryWithBackoff(ctx context.Context, fn func() error, maxRetries int) error {
	var err error
	backoff := time.Second

	for i := 0; i < maxRetries; i++ {
		err = fn()
		if err == nil {
			return nil
		}

		// Check if context was cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Log retry attempt
		log.Printf("Retry attempt %d/%d failed: %v. Retrying in %v...", i+1, maxRetries, err, backoff)

		// Wait for backoff period or context cancellation
		select {
		case <-time.After(backoff):
			backoff *= 2 // Exponential backoff
			if backoff > 30*time.Second {
				backoff = 30 * time.Second // Cap at 30 seconds
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("operation failed after %d retries: %w", maxRetries, err)
}
