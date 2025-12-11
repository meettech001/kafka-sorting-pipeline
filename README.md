# **Kafka Sorting Pipeline (Golang)**

This project implements a **high-performance data generation and sorting pipeline** in Golang.

The pipeline:

1. Generates **50 million** random CSV records following a fixed schema.
2. Produces them to a Kafka topic `source`.
3. Consumes the entire dataset from Kafka into a local flat file.
4. Performs **external sorting** (chunked + k-way merge) on the dataset by:

   * `id` (numeric ascending)
   * `name` (alphabetical)
   * `continent` (alphabetical)
5. Streams each globally sorted sequence back into Kafka topics:

   * `id`
   * `name`
   * `continent`

The code is engineered for **performance**, **deterministic behavior**, and **memory efficiency**.

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

1. **Generator + Producer**

   * Generates random records matching schema
   * Produces them to Kafka topic `source` in large batches

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

4. **Runtime Reporting**

   * Generation + Kafka produce time
   * Consumption time
   * Sorting + producing time (for each key)
   * Total pipeline runtime

---

## **Algorithms and Design Choices**

### **Random Data Generation**

* Efficient UTF-8-safe string builder
* No commas → simple CSV parsing
* Uniform distribution of continents
* ID is generated via `rand.Int31()`

### **Kafka IO**

* Uses **segmentio/kafka-go** (pure Go client)
* Producer:

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
      external_sort.go  # External chunk sort + k-way merge implementation
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

---

# **How to Verify the Pipeline Functionality**

Follow the steps below to validate correctness and performance.

---

## **1. Build the Docker image**

```bash
cd kafka-sorting-pipeline
./scripts/build.sh
```

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

You may adjust:

* `RECORDS=5000` for quick testing
* `CHUNK_SIZE=500`
* Docker network name

Running the container:

* Generates the dataset
* Sends to Kafka
* Sorts by three keys
* Sends sorted outputs to topics
* Prints timing breakdown

---

## **4. Manual sanity checks**

### **Check ID ordering**

```bash
kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic id --from-beginning --max-messages 20
```

IDs must be **strictly ascending**.

---

### **Check NAME ordering**

```bash
kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic name --from-beginning --max-messages 20
```

Names must be **lexicographically sorted (a → z)**.

---

### **Check CONTINENT ordering**

```bash
kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic continent --from-beginning --max-messages 20
```

Valid alphabetical order:

```
Africa
Asia
Australia
Europe
North America
South America
```
