# **Kafka Sorting Pipeline (Golang)**

This project implements a **high-performance data generation and sorting pipeline** in Golang.

The pipeline:

The code is structured for **performance**, **memory efficiency**, **fault tolerance**, and **clarity**.

---

## **Schema**

Each generated record contains four fields:

* **id** (`int32`): random 32-bit integer
* **name** (`string`): lowercase English letters, length **10–15**
* **address** (`string`): alphanumeric + space, length **15–20**, **no commas**
* **continent** (`string`): one of

  * North America
  * Asia
  * South America
  * Europe
  * Africa
  * Australia

Example CSV rows:

```text
21,axxxxxxxxx,12 abc dfsf LdUE,Asia
2,bxxxxxxxxy,9282 abc sf LdAUE,Africa
```

---

## **Architecture Overview**

High-level workflow:

1. **Generator + Producer (Go + kafka-go)**
   - Uses a random generator that strictly respects the schema.
   - Streams records in batches (10k messages) into Kafka topic `source`.
   - Supports configurable retry mechanisms with exponential backoff.

2. **Consumer → Flat File**
   - A dedicated consumer reads exactly N records from `source` and writes
     them to a local file `source.csv` in the container's filesystem.
   - Using a flat file decouples Kafka IO from sorting and avoids re-reading
     from Kafka multiple times.
   - Supports offset management through consumer groups for fault tolerance.

2. **Consumer → Local Flat File**

   * Reads exactly `RECORDS` messages from Kafka
   * Writes them into `/tmp/pipeline-data/source.csv`

3. **External Sort Pipeline**
   For each sort key (`id`, `name`, `continent`):

   **Chunk Phase:**

   * Read `chunkSize` records into memory
   * Sort in-memory chunk
   * Write sorted chunk to disk

   **K-Way Merge Phase:**

   * Open all sorted chunk files
   * Merge using a min-heap keyed by the sort column
   * Stream the merged result directly to a Kafka output topic

4. **Real-time Streaming Mode**
   - Alternative streaming mode processes data in real-time with partition-aware processing.
   - Supports horizontal scaling through concurrent partition processing.
   - Processes data as it arrives rather than in batch mode.

5. **Runtime Reporting**
   - The pipeline measures and prints wall-clock time for:
     - Data generation + production
     - Consumption into file
     - Sort + produce (each key)
     - Overall runtime
   - Comprehensive metrics collection for monitoring and observability.

---

## Enhanced Features

### Fault Tolerance and Resilience
- **Offset Management**: Consumer groups for automatic offset tracking and recovery.
- **Retry Mechanisms**: Exponential backoff retry logic for all Kafka operations.
- **Graceful Shutdown**: Proper cleanup and resource management on interruption.

### Observability
- **Structured Logging**: Detailed logging with timestamps for debugging and monitoring.
- **Metrics Collection**: Comprehensive metrics for generation, consumption, sorting, and production.
- **Progress Tracking**: Periodic progress reports during long-running operations.

### Scalability
- **Partition-Aware Processing**: Concurrent processing of Kafka partitions for horizontal scaling.
- **Configurable Concurrency**: Adjustable concurrency levels for optimal resource utilization.
- **Memory Efficiency**: Controlled memory usage through chunked processing.

### Real-time Processing
- **Streaming Mode**: Process data in real-time as it arrives in Kafka topics.
- **Continuous Operation**: Run indefinitely, processing new data as it becomes available.
- **Partition-Level Parallelism**: Each Kafka partition processed by a dedicated goroutine.

---

## **Algorithms and Design Choices**

### **Random Data Generation**

* Efficient UTF-8-safe string builder
* No commas → simple CSV parsing
* Uniform distribution of continents
* ID is generated via `rand.Int31()`

### **Kafka IO**

- Uses the pure-Go `github.com/segmentio/kafka-go` client.
- Producer:
  - Batches of 10,000 messages to reduce overhead.
  - Snappy compression enabled.
  - Configurable acknowledgment levels.
- Consumer:
  - Supports both sequential reading and consumer group-based offset management.
  - Configurable buffer sizes for optimal throughput.

  * Batch size: 10,000
  * Snappy compression
  * RequiredAcks=0 (fastest possible)
* Consumer:

  * No offset commits
  * Deterministic sequential read

### **External Sort**

Since 50M rows cannot fit in 2GB RAM → **external merge sort**.

1. **Chunk Sort:**

   * Sort <2M rows at a time
   * Safe bounded memory usage

2. **K-way Merge:**

   * Heap holds only 1 row per chunk
   * Output streamed sequentially to Kafka

This is the same algorithm used by modern databases and distributed systems.

---

## **Project Layout**

```text
kafka-sorting-pipeline/
  cmd/
    pipeline/
      main.go           # Orchestrates full pipeline
  internal/
    data/
      record.go         # Record struct, random generator, CSV encode/decode
    kafkautil/
      kafka.go          # Kafka reader/writer helper functions
    sorter/
      external_sort.go  # External sort implementation (chunk + k-way merge)
    streaming/
      processor.go      # Real-time streaming processor with partition awareness
    metrics/
      metrics.go        # Metrics collection and reporting
  scripts/
    build.sh            # Build Docker image
    run.sh              # Example usage
    start.sh            # Entry point inside pipeline container
  Dockerfile
  go.mod
  README.md
```

---

## **Building the Docker Image**

### **Prerequisites**

* Docker
* Kafka cluster (e.g., via docker-compose)
* Minimum 2GB RAM + 4 CPU cores allocated to Docker

### **Build**

```bash
cd kafka-sorting-pipeline
./scripts/build.sh
```

This creates the Docker image:

```
kafka-sorting-pipeline:latest
```

The script will print a **ready-to-use `docker run` command**.

---

## **Running the Pipeline**

Start Kafka (example: via docker-compose), then run the printed command:

```bash
docker run --rm \
  --network=<your-docker-network> \
  -e KAFKA_BROKER=kafka:9092 \
  -e RECORDS=50000000 \
  -e CHUNK_SIZE=2000000 \
  kafka-sorting-pipeline:latest
```

This will:

1. Wait for Kafka
2. Generate N records
3. Produce to topic `source`
4. Consume to `/tmp/pipeline-data/source.csv`
5. Perform all three sorts
6. Produce sorted results
7. Print detailed timing metrics

### Streaming Mode

To run in real-time streaming mode:

```bash
docker-compose run --rm pipeline -streaming=true -concurrency=4
```

In streaming mode:
- The pipeline continuously processes data from Kafka topics
- Each partition is processed by a separate goroutine
- Data is sorted and produced in real-time as it arrives
- Supports graceful shutdown with Ctrl+C

---

# **How to Verify the Pipeline Functionality**

Follow the steps below to validate correctness and performance.

---

## **1. Build the Docker image**

### What is Optimized

- **Batching & Compression**
  - Kafka producer batches 10,000 messages at a time and uses Snappy compression.
- **Sequential IO**
  - Flat file writes/reads are buffered (1 MiB buffers) and sequential, which is efficient on disk.
- **External Merge Sort**
  - Memory bound is controlled by `CHUNK_SIZE`. You can tune it based on the available memory.
- **Heap-based Merge**
  - K-way merge uses a minimal comparison per record per chunk, which is optimal for large sorted runs.
- **Retry Logic**
  - Exponential backoff for transient failures improves reliability.
- **Partition-Level Parallelism**
  - Concurrent processing of Kafka partitions maximizes throughput.

### Where the Major Bottlenecks Are

For large data volumes (50M rows), the main bottlenecks are:

1. **Disk IO During External Sort**
   - Reading and writing large chunk files dominates CPU time.
2. **Kafka Network IO**
   - Producing and consuming 50M messages is also heavy but can be mitigated with batching and compression.
3. **Serialization Overhead**
   - CSV encoding/decoding per record is relatively cheap but still non-trivial at this scale.

---

## **2. Copy the generated Docker run command**

The build script prints something like:

```bash
docker run --rm \
  --network=<your-docker-network> \
  -e KAFKA_BROKER=kafka:9092 \
  -e RECORDS=50000000 \
  -e CHUNK_SIZE=2000000 \
  kafka-sorting-pipeline:latest
```

Copy it.

---

## **3. Execute the command (modify if needed)**

- This repository is designed to be self-contained for the Golang app and its Docker image.
- The Kafka cluster is expected to be provided by the environment (e.g., via `docker-compose` as shown above).
- You can tune:
  - `RECORDS` (number of generated records),
  - `CHUNK_SIZE` (records per in-memory chunk),
  - Kafka batch and buffer sizes (by editing `internal/kafkautil/kafka.go`).
- The streaming mode provides real-time processing capabilities for continuous data flows.
