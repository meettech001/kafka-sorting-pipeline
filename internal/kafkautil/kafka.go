package kafkautil

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// NewWriter creates a tuned Kafka writer.
func NewWriter(broker, topic string, requiredAcks int) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequiredAcks(requiredAcks), // 0 = no ack, 1 = leader, -1 = all replicas
		BatchSize:    10_000,
		BatchTimeout: 500 * time.Millisecond,
		Compression:  kafka.Snappy,
		Async:        false,
	}
}

// NewPartitionedWriter creates a Kafka writer with explicit partitioning support.
func NewPartitionedWriter(broker, topic string, requiredAcks int) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // Use hash balancer for consistent partitioning
		RequiredAcks: kafka.RequiredAcks(requiredAcks),
		BatchSize:    10_000,
		BatchTimeout: 500 * time.Millisecond,
		Compression:  kafka.Snappy,
		Async:        false,
	}
}

// NewReader creates a tuned Kafka reader.
func NewReader(broker, topic string, minBytes int) *kafka.Reader {
	if minBytes <= 0 {
		minBytes = 1 << 20 // 1 MiB
	}
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          topic,
		GroupID:        "",
		MinBytes:       minBytes,
		MaxBytes:       10 << 20, // 10 MiB
		CommitInterval: 0,        // no commits, we read sequentially
	})
}

// NewConsumerGroupReader creates a Kafka reader with consumer group support for offset management.
func NewConsumerGroupReader(broker, topic, groupID string, minBytes int) *kafka.Reader {
	if minBytes <= 0 {
		minBytes = 1 << 20 // 1 MiB
	}
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       minBytes,
		MaxBytes:       10 << 20, // 10 MiB
		CommitInterval: 1 * time.Second,
	})
}

// NewPartitionedReader creates a Kafka reader for specific partitions.
func NewPartitionedReader(broker, topic string, partition int, minBytes int) *kafka.Reader {
	if minBytes <= 0 {
		minBytes = 1 << 20 // 1 MiB
	}
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          topic,
		Partition:      partition,
		MinBytes:       minBytes,
		MaxBytes:       10 << 20, // 10 MiB
		CommitInterval: 1 * time.Second,
	})
}
