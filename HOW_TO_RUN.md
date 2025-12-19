Here are the steps to run the improved Kafka sorting pipeline on your terminal:

## Prerequisites
Make sure you have Docker and Docker Compose installed on your system.

## Steps to Run the Pipeline

1. **Navigate to the project directory:**
```bash
cd /home/mitesh/Projects/go/kafka-sorting-pipeline
```

2. **Build the Docker images:**
```bash
docker compose build
```

3. **Start all services:**
```bash
docker compose up -d
```

4. **Monitor the pipeline progress:**
```bash
docker compose logs -f pipeline
```

5. **Check if topics were created successfully:**
```bash
docker compose logs kafka-init
```

6. **To stop the services:**
```bash
docker compose down
```

## Running in Streaming Mode

If you want to run the pipeline in real-time streaming mode:

1. **Start only Kafka and Zookeeper:**
```bash
docker compose up -d zookeeper kafka kafka-init
```

2. **Wait for initialization to complete, then run the pipeline in streaming mode:**
```bash
docker compose run --rm -p 8080:8080 pipeline /app/pipeline -streaming=true -concurrency=4
```

## Viewing Results

To verify the results of the sorting pipeline:

1. **List all topics:**
```bash
docker exec -it kafka-sorting-pipeline-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
```

2. **View messages in a sorted topic (e.g., by ID):**
```bash
docker exec -it kafka-sorting-pipeline-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic id --from-beginning --max-messages 10
```

3. **Check record counts in each topic:**
```bash
docker exec -it kafka-sorting-pipeline-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic source --time -1
docker exec -it kafka-sorting-pipeline-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic id --time -1
docker exec -it kafka-sorting-pipeline-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic name --time -1
docker exec -it kafka-sorting-pipeline-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic continent --time -1
```

## Adjusting Parameters

You can adjust various parameters by modifying the environment variables in the `docker-compose.yml` file:

- `RECORDS`: Number of records to generate (default: 50000000)
- `CHUNK_SIZE`: Records per in-memory chunk during external sort (default: 2000000)
- `KAFKA_BROKER`: Kafka broker address (default: kafka:9092)

Or run with custom parameters:
```bash
docker docker compose run --rm -e RECORDS=100000 -e CHUNK_SIZE=1000 pipeline

```


```bash

check metrics here:
http://localhost:9090/metrics
Metrics

pipeline_records_generated_total
pipeline_records_consumed_total
pipeline_records_sorted_total
pipeline_records_produced_total
pipeline_generation_errors_total
pipeline_consumption_errors_total
pipeline_sorting_errors_total
pipeline_production_errors_total
```
The pipeline will automatically create all required Kafka topics and handle the sorting process with the improvements we've implemented for fault tolerance, scalability, and observability.