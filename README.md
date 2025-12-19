# Kafka Sorting Pipeline (Golang)

This project implements a **data generation and processing pipeline** in Golang:

1. Generate **50 million** random CSV records following the required schema.
2. Produce them to a Kafka topic `source`.
3. Consume all records from `source` into a flat file (for efficient local processing).
4. Perform **external sort** (chunked + k-way merge) of the data by:
   - `id` (numeric)
   - `name` (alphabetical)
   - `continent` (alphabetical)
5. For each sort order, stream the globally sorted sequence back to Kafka into
   three topics: `id`, `name`, `continent`.

The code is structured for **performance**, **memory efficiency**, **fault tolerance**, and **clarity**.

---

## Schema

Each record has the following fields:

- `id` (int32): integer within 32-bit range.
- `name` (string): English letters only, length in `[10, 15]`.
- `address` (string): mixture of letters, digits, and spaces, length in `[15, 20]` (no commas).
- `continent` (string): one of:
  - `North America`
  - `Asia`
  - `South America`
  - `Europe`
  - `Africa`
  - `Australia`

CSV examples:

```text
21,axxxxxxxxx,12 abc dfsf LdUE,Asia
2,bxxxxxxxxy,9282 abc sf LdAUE,Africa
```

---

## Architecture Overview

High-level flow:

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

3. **External Sort (per key) + Producer**
   For each key (`id`, `name`, `continent`):

   a. **Chunk Sorting Phase**
   - Stream through `source.csv`, accumulating up to `chunkSize` records in memory.
   - Sort the in-memory chunk using `sort.Slice` with a key-specific comparator.
   - Write the sorted chunk to a temporary file under a dedicated chunk directory.

   b. **K-way Merge Phase**
   - Open all chunk files.
   - Use a min-heap (priority queue) keyed by the sort key to perform a k-way merge.
   - As we pop the smallest record from the heap, we immediately send it as a Kafka
     message to the corresponding output topic (`id`, `name`, or `continent`).
   - This ensures we never hold the entire dataset in RAM at once.

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

## Algorithms and Design Choices

### Random Data Generation

- The generator uses `math/rand` with a per-process seed.
- `id` is generated as `int32(rand.Int31())`.
- `name` is generated with lowercase letters `[a-z]`, length `[10, 15]`.
- `address` uses characters `[a-zA-Z0-9 ]`, length `[15, 20]`. Commas are explicitly avoided so the CSV can be parsed with a simple split.
- `continent` is randomly chosen from the six allowed values.

### Kafka IO

- Uses the pure-Go `github.com/segmentio/kafka-go` client.
- Producer:
  - Batches of 10,000 messages to reduce overhead.
  - Snappy compression enabled.
  - Configurable acknowledgment levels.
- Consumer:
  - Supports both sequential reading and consumer group-based offset management.
  - Configurable buffer sizes for optimal throughput.

### External Sort

Given 50M records and a strict memory budget (2GB including Kafka), a naive in-memory sort is not safe.

We implement:

1. **Chunk Sorting:**
   - Read up to `chunkSize` (e.g., 2,000,000) records into memory.
   - Sort the slice with `sort.Slice`.
   - Write a sorted chunk to disk.

2. **K-way Merge:**
   - Maintain a min-heap where each element is the current head record of a chunk file.
   - Repeatedly pop the smallest record and push the next record from that chunk.
   - Stream the merged output directly into Kafka as messages.

This is a standard external merge sort pattern and scales beyond what fits in RAM.

---

## Project Layout

```text
kafka-sorting-pipeline/
  cmd/
    pipeline/
      main.go           # Orchestrates the whole pipeline
  internal/
    data/
      record.go         # Record definition, CSV encode/decode, random generator
    kafkautil/
      kafka.go          # Kafka reader/writer helpers
    sorter/
      external_sort.go  # External sort implementation (chunk + k-way merge)
    streaming/
      processor.go      # Real-time streaming processor with partition awareness
    metrics/
      metrics.go        # Metrics collection and reporting
  scripts/
    build.sh            # Build Docker image
    run.sh              # Example how to run the container
    start.sh            # Entry-point inside the container
  Dockerfile
  go.mod
  README.md
```

---

## Building the Docker Image

Prerequisites on the host:

- Docker (with at least 2GB memory and 4 cores allocated to Docker)
- A Kafka cluster (Dockerized example below)

Build the image:

```bash
cd kafka-sorting-pipeline
./scripts/build.sh
# or:
IMAGE_NAME=myuser/kafka-sorting-pipeline IMAGE_TAG=latest ./scripts/build.sh
```

---

## Running with Docker + Kafka

You need a Kafka broker reachable from the container. A minimal `docker-compose.yaml` example:

```yaml
version: "3.8"

services:
  zookeeper:
    image: bitnami/zookeeper:latest
    environment:
      - ALLOW_ANONYMOUS_LOGIN=yes
    ports:
      - "2181:2181"
    mem_limit: 256m

  kafka:
    image: bitnami/kafka:latest
    ports:
      - "9092:9092"
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092
    depends_on:
      - zookeeper
    mem_limit: 1g

  pipeline:
    image: kafka-sorting-pipeline:latest
    environment:
      - KAFKA_BROKER=kafka:9092
      - RECORDS=50000000        # Adjust for testing
      - CHUNK_SIZE=2000000      # Adjust to control memory usage
    depends_on:
      - kafka
    mem_limit: 512m
    cpus: "4.0"
```

Run:

```bash
docker-compose up --build
```

The `pipeline` container will:

1. Wait until `kafka:9092` is reachable.
2. Generate and send the configured number of records to `source`.
3. Consume them into `/tmp/pipeline-data/source.csv`.
4. Sort and write results to Kafka topics `id`, `name`, `continent`.
5. Print timing breakdowns to stdout.

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

## Verifying Correctness

Inside or outside the Docker network, you can verify:

1. **Record Counts**

   Each output topic (`id`, `name`, `continent`) should contain exactly the same
   number of messages as `source`.

   Example using `kafka-console-consumer` or any Kafka client of your choice.

2. **Order by Key**

   - For `id` topic:
     - Scan messages sequentially and ensure each `id` is `>=` the previous `id`.
   - For `name` topic:
     - Ensure lexical order of `name`, with `id` as a tie-breaker.
   - For `continent` topic:
     - Ensure lexical order of `continent`, with `id` as a tie-breaker.

   For quick sanity checks, you can consume the first N messages from each topic
   and visually inspect them.

3. **Schema Validation**

   - Ensure every line has exactly four comma-separated fields.
   - Validate field constraints (lengths, allowed characters, continent values).

For development/testing, you can reduce `RECORDS` to a smaller number (e.g., `100000`) to speed things up and validate behavior.

---

## Performance and Optimizations

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

## Scaling Further: More Data, More Machines

If we had more data (e.g., billions of rows) and more machines:

1. **Partitioned Sorting**
   - Partition input data by key range or hash across multiple nodes.
   - Each node performs an external sort for its partition.
   - The global sorted order is obtained by concatenating the sorted partitions.

2. **Kafka Partitions**
   - Use many partitions per topic, each processed by separate instances of the sorter service.
   - Each instance performs local external sorting; combine results if a fully global order is required.

3. **Distributed Filesystem**
   - Use a distributed filesystem (e.g., HDFS, S3) to store chunk files.
   - This allows horizontal scaling of storage and IO bandwidth.

4. **Binary Format Instead of CSV**
   - Switch to a compact binary format (e.g., Protobuf / Avro) to reduce serialization overhead and network usage.

5. **Streaming Frameworks**
   - Integrate with Apache Flink / Kafka Streams for built-in windowed and partitioned sorting, if exact global order is not strictly required.

---

## Notes

- This repository is designed to be self-contained for the Golang app and its Docker image.
- The Kafka cluster is expected to be provided by the environment (e.g., via `docker-compose` as shown above).
- You can tune:
  - `RECORDS` (number of generated records),
  - `CHUNK_SIZE` (records per in-memory chunk),
  - Kafka batch and buffer sizes (by editing `internal/kafkautil/kafka.go`).
- The streaming mode provides real-time processing capabilities for continuous data flows.