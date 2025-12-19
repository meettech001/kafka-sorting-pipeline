package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"

	"github.com/example/kafka-sorting-pipeline/internal/data"
	"github.com/example/kafka-sorting-pipeline/internal/kafkautil"
	"github.com/example/kafka-sorting-pipeline/internal/metrics"
	"github.com/example/kafka-sorting-pipeline/internal/sorter"
	"github.com/example/kafka-sorting-pipeline/internal/streaming"
)

// Prometheus metrics
var (
	recordsGenerated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pipeline_records_generated_total",
		Help: "The total number of records generated",
	})
	recordsConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pipeline_records_consumed_total",
		Help: "The total number of records consumed",
	})
	recordsSorted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pipeline_records_sorted_total",
		Help: "The total number of records sorted",
	})
	recordsProduced = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pipeline_records_produced_total",
		Help: "The total number of records produced",
	})
	generationErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pipeline_generation_errors_total",
		Help: "The total number of generation errors",
	})
	consumptionErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pipeline_consumption_errors_total",
		Help: "The total number of consumption errors",
	})
	sortingErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pipeline_sorting_errors_total",
		Help: "The total number of sorting errors",
	})
	productionErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pipeline_production_errors_total",
		Help: "The total number of production errors",
	})
	generationDuration = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pipeline_generation_duration_seconds",
		Help: "Duration of the generation phase in seconds",
	})
	consumptionDuration = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pipeline_consumption_duration_seconds",
		Help: "Duration of the consumption phase in seconds",
	})
	sortingDuration = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pipeline_sorting_duration_seconds",
		Help: "Duration of the sorting phase in seconds",
	})
	productionDuration = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pipeline_production_duration_seconds",
		Help: "Duration of the production phase in seconds",
	})
)

type timings struct {
	GenerateProduce time.Duration
	ConsumeSource   time.Duration
	SortID          time.Duration
	SortName        time.Duration
	SortContinent   time.Duration
}

func main() {
	var (
		broker        = flag.String("broker", "kafka:9092", "Kafka bootstrap broker")
		sourceTopic   = flag.String("source-topic", "source", "Source topic name")
		idTopic       = flag.String("id-topic", "id", "Sorted-by-id topic name")
		nameTopic     = flag.String("name-topic", "name", "Sorted-by-name topic name")
		contTopic     = flag.String("continent-topic", "continent", "Sorted-by-continent topic name")
		numRecords    = flag.Int("records", 50_000_000, "Number of records to generate (0 for continuous streaming)")
		chunkSize     = flag.Int("chunk-size", 2_000_000, "Number of records per in-memory chunk during external sort")
		workDir       = flag.String("workdir", "/tmp/pipeline-data", "Working directory for temporary files")
		skipGenerate  = flag.Bool("skip-generate", false, "Skip generation step and only sort existing source topic")
		skipSort      = flag.Bool("skip-sort", false, "Skip sort step (only generate)")
		groupID       = flag.String("group-id", "sorting-pipeline-consumer", "Kafka consumer group ID for offset management")
		streamingMode = flag.Bool("streaming", false, "Enable streaming mode for real-time processing")
		concurrency   = flag.Int("concurrency", 4, "Concurrency level for streaming processing")
		metricsPort   = flag.String("metrics-port", ":8080", "Port for Prometheus metrics endpoint")
	)
	flag.Parse()

	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log.Printf("Starting pipeline with %d records, chunk size %d", *numRecords, *chunkSize)
	log.Printf("Go version: %s, NumCPU=%d", runtime.Version(), runtime.NumCPU())

	// Start Prometheus metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("Prometheus metrics server starting on %s", *metricsPort)
		if err := http.ListenAndServe(*metricsPort, nil); err != nil {
			log.Printf("Prometheus metrics server error: %v", err)
		}
	}()

	if err := os.MkdirAll(*workDir, 0o755); err != nil {
		log.Fatalf("Failed to create work directory: %v", err)
	}

	// Set up context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT and SIGTERM for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal %s, initiating graceful shutdown...", sig)
		cancel()

		// Wait a bit for graceful shutdown
		time.Sleep(2 * time.Second)
		log.Println("Forcefully exiting...")
		os.Exit(1)
	}()

	// Initialize metrics
	appMetrics := metrics.NewMetrics()

	// Ensure Kafka is reachable before starting
	if err := waitForKafka(ctx, *broker); err != nil {
		log.Fatalf("Failed to connect to Kafka: %v", err)
	}

	// If streaming mode is enabled, run streaming processor instead
	if *streamingMode {
		log.Printf("Running in streaming mode with concurrency level %d", *concurrency)

		// If numRecords is 0, run in continuous streaming mode
		if *numRecords == 0 {
			log.Printf("Running in continuous streaming mode (press Ctrl+C to stop)")
			processor := streaming.NewStreamProcessor(
				*broker, *sourceTopic, *idTopic, *nameTopic, *contTopic,
				*groupID, *chunkSize, *workDir, *concurrency, appMetrics)

			if err := processor.ProcessStream(ctx); err != nil {
				log.Fatalf("Streaming processing failed: %v", err)
			}

			appMetrics.Print()
			return
		} else {
			// Run with fixed record count
			processor := streaming.NewStreamProcessor(
				*broker, *sourceTopic, *idTopic, *nameTopic, *contTopic,
				*groupID, *chunkSize, *workDir, *concurrency, appMetrics)

			if err := processor.ProcessStream(ctx); err != nil {
				log.Fatalf("Streaming processing failed: %v", err)
			}

			appMetrics.Print()
			return
		}
	}

	t := &timings{}
	overallStart := time.Now()

	if !*skipGenerate {
		start := time.Now()
		if err := runGenerate(ctx, *broker, *sourceTopic, *numRecords, appMetrics); err != nil {
			log.Fatalf("Generate/produce failed: %v", err)
		}
		t.GenerateProduce = time.Since(start)
		generationDuration.Set(t.GenerateProduce.Seconds())
		log.Printf("[TIMING] generate+produce: %s", t.GenerateProduce)
	} else {
		log.Printf("Skipping generation as requested")
	}

	if *skipSort {
		printTimings(time.Since(overallStart), t)
		appMetrics.Print()
		return
	}

	sourceFile := fmt.Sprintf("%s/source.csv", *workDir)

	// Consume source topic into a flat file once; re-use it for all sort keys.
	startConsume := time.Now()
	if err := runConsume(ctx, *broker, *sourceTopic, sourceFile, *numRecords, *groupID, appMetrics); err != nil {
		log.Fatalf("Consume source failed: %v", err)
	}
	t.ConsumeSource = time.Since(startConsume)
	consumptionDuration.Set(t.ConsumeSource.Seconds())
	log.Printf("[TIMING] consume source: %s", t.ConsumeSource)

	// Sort by id
	startID := time.Now()
	if err := runSortAndProduce(ctx, *broker, sourceFile, *idTopic, *chunkSize, sorter.SortByID, appMetrics); err != nil {
		log.Fatalf("Sort by id failed: %v", err)
	}
	t.SortID = time.Since(startID)
	sortingDuration.Set(t.SortID.Seconds())
	log.Printf("[TIMING] sort+produce by id: %s", t.SortID)

	// Sort by name
	startName := time.Now()
	if err := runSortAndProduce(ctx, *broker, sourceFile, *nameTopic, *chunkSize, sorter.SortByName, appMetrics); err != nil {
		log.Fatalf("Sort by name failed: %v", err)
	}
	t.SortName = time.Since(startName)
	sortingDuration.Set(t.SortName.Seconds())
	log.Printf("[TIMING] sort+produce by name: %s", t.SortName)

	// Sort by continent
	startCont := time.Now()
	if err := runSortAndProduce(ctx, *broker, sourceFile, *contTopic, *chunkSize, sorter.SortByContinent, appMetrics); err != nil {
		log.Fatalf("Sort by continent failed: %v", err)
	}
	t.SortContinent = time.Since(startCont)
	sortingDuration.Set(t.SortContinent.Seconds())
	log.Printf("[TIMING] sort+produce by continent: %s", t.SortContinent)

	overall := time.Since(overallStart)
	printTimings(overall, t)
	appMetrics.Print()
}

func waitForKafka(ctx context.Context, broker string) error {
	log.Printf("Waiting for Kafka at %s ...", broker)

	// Retry with exponential backoff
	backoff := time.Second
	maxRetries := 30 // Increased retry count

	for i := 0; i < maxRetries; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			conn, err := kafka.DialContext(ctx, "tcp", broker)
			if err == nil {
				_ = conn.Close()
				log.Printf("Kafka reachable at %s", broker)
				return nil
			}

			log.Printf("Kafka not ready (%v), retrying in %v", err, backoff)

			select {
			case <-time.After(backoff):
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return fmt.Errorf("failed to connect to Kafka after %d attempts", maxRetries)
}

func runGenerate(ctx context.Context, broker, sourceTopic string, numRecords int, metrics *metrics.Metrics) error {
	log.Printf("Generating %d records and producing to topic %q", numRecords, sourceTopic)

	writer := kafkautil.NewWriter(broker, sourceTopic, 0)
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("Warning: error closing writer: %v", err)
		}
	}()

	const batchSize = 10_000
	records := make([]data.Record, batchSize)
	msgs := make([]kafka.Message, 0, batchSize)

	gen := data.NewGenerator()

	written := 0
	start := time.Now()

	// Use a ticker to report progress
	progressTicker := time.NewTicker(30 * time.Second)
	defer progressTicker.Stop()

	for written < numRecords {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-progressTicker.C:
			if written > 0 {
				elapsed := time.Since(start)
				rate := float64(written) / elapsed.Seconds()
				log.Printf("Progress: %d/%d records generated (%.0f rec/s)", written, numRecords, rate)
			}
		default:
			n := batchSize
			if numRecords-written < batchSize {
				n = numRecords - written
			}
			for i := 0; i < n; i++ {
				records[i] = gen.RandomRecord()
			}

			msgs = msgs[:0]
			for i := 0; i < n; i++ {
				line := records[i].ToCSV()
				msgs = append(msgs, kafka.Message{Value: []byte(line)})
			}

			// Implement retry mechanism with exponential backoff
			if err := retryWithBackoff(ctx, func() error {
				return writer.WriteMessages(ctx, msgs...)
			}, 5); err != nil {
				metrics.IncrementGenerationErrors()
				generationErrors.Inc()
				return fmt.Errorf("write messages after retries: %w", err)
			}

			metrics.IncrementRecordsGenerated(n)
			recordsGenerated.Add(float64(n))
			written += n
			if written%1_000_000 == 0 {
				elapsed := time.Since(start)
				log.Printf("Generated %d records in %s (%.0f rec/s)", written, elapsed, float64(written)/elapsed.Seconds())
			}
		}
	}

	log.Printf("Successfully generated %d records", written)
	return nil
}

func runConsume(ctx context.Context, broker, topic, destFile string, expected int, groupID string, metrics *metrics.Metrics) error {
	log.Printf("Consuming %d records from topic %q into %s with consumer group %s", expected, topic, destFile, groupID)

	// Use consumer group reader for offset management
	reader := kafkautil.NewConsumerGroupReader(broker, topic, groupID, 0)
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Warning: error closing reader: %v", err)
		}
	}()

	f, err := os.Create(destFile)
	if err != nil {
		metrics.IncrementConsumptionErrors()
		consumptionErrors.Inc()
		return fmt.Errorf("create dest file: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("Warning: error closing file: %v", err)
		}
	}()

	w := bufio.NewWriterSize(f, 1<<20)
	defer func() {
		if err := w.Flush(); err != nil {
			log.Printf("Warning: error flushing buffer: %v", err)
		}
	}()

	count := 0

	// Use a ticker to report progress
	progressTicker := time.NewTicker(30 * time.Second)
	defer progressTicker.Stop()

	for count < expected {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-progressTicker.C:
			if count > 0 {
				log.Printf("Progress: consumed %d/%d records", count, expected)
			}
		default:
			// Implement retry mechanism with exponential backoff
			var msg kafka.Message
			if err := retryWithBackoff(ctx, func() error {
				m, err := reader.ReadMessage(ctx)
				if err != nil {
					return err
				}
				msg = m
				return nil
			}, 5); err != nil {
				metrics.IncrementConsumptionErrors()
				consumptionErrors.Inc()
				return fmt.Errorf("read message after retries: %w", err)
			}

			if _, err := w.Write(msg.Value); err != nil {
				metrics.IncrementConsumptionErrors()
				consumptionErrors.Inc()
				return fmt.Errorf("write value: %w", err)
			}
			if err := w.WriteByte('\n'); err != nil {
				metrics.IncrementConsumptionErrors()
				consumptionErrors.Inc()
				return fmt.Errorf("write newline: %w", err)
			}
			metrics.IncrementRecordsConsumed()
			recordsConsumed.Inc()
			count++
			if count%1_000_000 == 0 {
				log.Printf("Consumed %d messages", count)
			}
		}
	}

	log.Printf("Finished consuming %d records into %s", count, destFile)
	return nil
}

func runSortAndProduce(ctx context.Context, broker, srcFile, topic string, chunkSize int, key sorter.SortKey, metrics *metrics.Metrics) error {
	log.Printf("Sorting %s by %s and producing to topic %q", srcFile, key, topic)
	tmpDir := fmt.Sprintf("%s-%s-chunks", srcFile, key)
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		metrics.IncrementSortingErrors()
		sortingErrors.Inc()
		return fmt.Errorf("mkdir tmp dir: %w", err)
	}

	writer := kafkautil.NewWriter(broker, topic, 1)
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("Warning: error closing writer: %v", err)
		}
	}()

	// Create a wrapper function to track metrics for the external sort
	sortFunc := func() error {
		return sorter.ExternalSortAndProduce(ctx, srcFile, tmpDir, chunkSize, key, writer)
	}

	if err := retryWithBackoff(ctx, sortFunc, 3); err != nil {
		metrics.IncrementSortingErrors()
		sortingErrors.Inc()
		return fmt.Errorf("external sort after retries: %w", err)
	}

	// Best-effort cleanup
	if err := os.RemoveAll(tmpDir); err != nil {
		log.Printf("warning: failed to remove tmpdir %s: %v", tmpDir, err)
	}

	return nil
}

func printTimings(overall time.Duration, t *timings) {
	log.Println("===================================================")
	log.Println("Pipeline runtime summary")
	log.Println("===================================================")
	log.Printf("Generate + Produce : %s", t.GenerateProduce)
	log.Printf("Consume Source     : %s", t.ConsumeSource)
	log.Printf("Sort + Produce ID  : %s", t.SortID)
	log.Printf("Sort + Produce Name: %s", t.SortName)
	log.Printf("Sort + Produce Cont: %s", t.SortContinent)
	log.Println("---------------------------------------------------")
	log.Printf("Overall runtime    : %s", overall)
	log.Println("===================================================")
}

// retryWithBackoff implements exponential backoff retry mechanism
func retryWithBackoff(ctx context.Context, fn func() error, maxRetries int) error {
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
