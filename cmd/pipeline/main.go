package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/example/kafka-sorting-pipeline/internal/data"
	"github.com/example/kafka-sorting-pipeline/internal/kafkautil"
	"github.com/example/kafka-sorting-pipeline/internal/sorter"
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
		broker       = flag.String("broker", "kafka:9092", "Kafka bootstrap broker")
		sourceTopic  = flag.String("source-topic", "source", "Source topic name")
		idTopic      = flag.String("id-topic", "id", "Sorted-by-id topic name")
		nameTopic    = flag.String("name-topic", "name", "Sorted-by-name topic name")
		contTopic    = flag.String("continent-topic", "continent", "Sorted-by-continent topic name")
		numRecords   = flag.Int("records", 50_000_000, "Number of records to generate")
		chunkSize    = flag.Int("chunk-size", 2_000_000, "Number of records per in-memory chunk during external sort")
		workDir      = flag.String("workdir", "/tmp/pipeline-data", "Working directory for temporary files")
		skipGenerate = flag.Bool("skip-generate", false, "Skip generation step and only sort existing source topic")
		skipSort     = flag.Bool("skip-sort", false, "Skip sort step (only generate)")
	)
	flag.Parse()

	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log.Printf("Starting pipeline with %d records, chunk size %d", *numRecords, *chunkSize)
	log.Printf("Go version: %s, NumCPU=%d", runtime.Version(), runtime.NumCPU())

	if err := os.MkdirAll(*workDir, 0o755); err != nil {
		log.Fatalf("creating workdir: %v", err)
	}

	ctx := context.Background()

	// Ensure Kafka is reachable before starting
	waitForKafka(ctx, *broker)

	t := &timings{}
	overallStart := time.Now()

	if !*skipGenerate {
		start := time.Now()
		if err := runGenerate(ctx, *broker, *sourceTopic, *numRecords); err != nil {
			log.Fatalf("generate/produce failed: %v", err)
		}
		t.GenerateProduce = time.Since(start)
		log.Printf("[TIMING] generate+produce: %s", t.GenerateProduce)
	} else {
		log.Printf("Skipping generation as requested")
	}

	if *skipSort {
		printTimings(time.Since(overallStart), t)
		return
	}

	sourceFile := fmt.Sprintf("%s/source.csv", *workDir)

	// Consume source topic into a flat file once; re-use it for all sort keys.
	startConsume := time.Now()
	if err := runConsume(ctx, *broker, *sourceTopic, sourceFile, *numRecords); err != nil {
		log.Fatalf("consume source failed: %v", err)
	}
	t.ConsumeSource = time.Since(startConsume)
	log.Printf("[TIMING] consume source: %s", t.ConsumeSource)

	// Sort by id
	startID := time.Now()
	if err := runSortAndProduce(ctx, *broker, sourceFile, *idTopic, *chunkSize, sorter.SortByID); err != nil {
		log.Fatalf("sort by id failed: %v", err)
	}
	t.SortID = time.Since(startID)
	log.Printf("[TIMING] sort+produce by id: %s", t.SortID)

	// Sort by name
	startName := time.Now()
	if err := runSortAndProduce(ctx, *broker, sourceFile, *nameTopic, *chunkSize, sorter.SortByName); err != nil {
		log.Fatalf("sort by name failed: %v", err)
	}
	t.SortName = time.Since(startName)
	log.Printf("[TIMING] sort+produce by name: %s", t.SortName)

	// Sort by continent
	startCont := time.Now()
	if err := runSortAndProduce(ctx, *broker, sourceFile, *contTopic, *chunkSize, sorter.SortByContinent); err != nil {
		log.Fatalf("sort by continent failed: %v", err)
	}
	t.SortContinent = time.Since(startCont)
	log.Printf("[TIMING] sort+produce by continent: %s", t.SortContinent)

	overall := time.Since(overallStart)
	printTimings(overall, t)
}

func waitForKafka(ctx context.Context, broker string) {
	log.Printf("Waiting for Kafka at %s ...", broker)
	for {
		conn, err := kafka.DialContext(ctx, "tcp", broker)
		if err == nil {
			_ = conn.Close()
			log.Printf("Kafka reachable at %s", broker)
			return
		}
		log.Printf("Kafka not ready (%v), retrying in 3s", err)
		time.Sleep(3 * time.Second)
	}
}

func runGenerate(ctx context.Context, broker, sourceTopic string, numRecords int) error {
	log.Printf("Generating %d records and producing to topic %q", numRecords, sourceTopic)

	writer := kafkautil.NewWriter(broker, sourceTopic, 0)
	defer writer.Close()

	const batchSize = 10_000
	records := make([]data.Record, batchSize)
	msgs := make([]kafka.Message, 0, batchSize)

	gen := data.NewGenerator()

	written := 0
	start := time.Now()
	for written < numRecords {
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

		if err := writer.WriteMessages(ctx, msgs...); err != nil {
			return fmt.Errorf("write messages: %w", err)
		}

		written += n
		if written%1_000_000 == 0 {
			elapsed := time.Since(start)
			log.Printf("Generated %d records in %s (%.0f rec/s)", written, elapsed, float64(written)/elapsed.Seconds())
		}
	}

	return nil
}

func runConsume(ctx context.Context, broker, topic, destFile string, expected int) error {
	log.Printf("Consuming %d records from topic %q into %s", expected, topic, destFile)

	reader := kafkautil.NewReader(broker, topic, 0)
	defer reader.Close()

	f, err := os.Create(destFile)
	if err != nil {
		return fmt.Errorf("create dest file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 1<<20)
	defer w.Flush()

	count := 0
	for count < expected {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("read message: %w", err)
		}
		if _, err := w.Write(msg.Value); err != nil {
			return fmt.Errorf("write value: %w", err)
		}
		if err := w.WriteByte('\n'); err != nil {
			return fmt.Errorf("write newline: %w", err)
		}
		count++
		if count%1_000_000 == 0 {
			log.Printf("Consumed %d messages", count)
		}
	}

	log.Printf("Finished consuming %d records into %s", count, destFile)
	return nil
}

func runSortAndProduce(ctx context.Context, broker, srcFile, topic string, chunkSize int, key sorter.SortKey) error {
	log.Printf("Sorting %s by %s and producing to topic %q", srcFile, key, topic)
	tmpDir := fmt.Sprintf("%s-%s-chunks", srcFile, key)
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return fmt.Errorf("mkdir tmp dir: %w", err)
	}

	writer := kafkautil.NewWriter(broker, topic, 1)
	defer writer.Close()

	if err := sorter.ExternalSortAndProduce(ctx, srcFile, tmpDir, chunkSize, key, writer); err != nil {
		return fmt.Errorf("external sort: %w", err)
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
