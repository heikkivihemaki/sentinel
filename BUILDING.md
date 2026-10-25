# Building Sentinel

## Local build

Requires Java 21, Maven 3.9+. Docker and Docker Compose are required to run the infrastructure.

```bash
mvn clean verify
```

## Run everything in Docker

The Dockerfile copies the application JAR from `target`, so build it first:

```bash
mvn clean verify
docker compose up -d --build
```

## Run Sentinel locally with Docker infrastructure

If the Sentinel container is already running, stop it first:

```bash
docker compose stop sentinel
```

```bash
docker compose up -d kafka postgres redis prometheus grafana
mvn spring-boot:run
```

Kafka uses `kafka:9092` for Docker clients and `localhost:9092`
for local clients. Run only one Sentinel instance at a time
to avoid a port 8080 conflict.

## What to capture for a portfolio demo

- Kafka topic partition count
- event ingestion rate from k6
- p95/p99 HTTP latency
- detector processing latency
- anomaly count
- behaviour during detector restart
- behaviour during PostgreSQL outage

Only publish measurements you actually reproduce.
