# Multi-stage build: builder
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/pipeline ./cmd/pipeline

# Final runtime image
FROM alpine:3.20

# Minimal tooling for debugging / scripts
RUN apk add --no-cache bash ca-certificates

WORKDIR /app

COPY --from=builder /app/pipeline /app/pipeline
COPY scripts /app/scripts

ENV PATH="/app/scripts:${PATH}"

# Default configuration suitable for docker-compose with a "kafka" service
ENV KAFKA_BROKER=kafka:9092
ENV SOURCE_TOPIC=source
ENV ID_TOPIC=id
ENV NAME_TOPIC=name
ENV CONTINENT_TOPIC=continent
ENV RECORDS=50000000
ENV CHUNK_SIZE=2000000
ENV WORKDIR=/tmp/pipeline-data

ENTRYPOINT ["bash", "/app/scripts/start.sh"]