# Sentinel

[![Java](https://img.shields.io/badge/Java-21-orange?logo=openjdk)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.4-brightgreen?logo=springboot)](https://spring.io/projects/spring-boot)
[![Kafka Streams](https://img.shields.io/badge/Kafka%20Streams-stateful-black?logo=apachekafka)](https://kafka.apache.org/documentation/streams/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue?logo=postgresql)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Hacktoberfest](https://img.shields.io/badge/Hacktoberfest-2026-blueviolet)](https://hacktoberfest.com)
[![Good First Issues](https://img.shields.io/github/issues/taniyanandi/sentinel/good%20first%20issue)](https://github.com/taniyanandi/sentinel/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)

**A distributed, stateful stream processing engine that detects anomalies in high volume telemetry in near real time.**

Sentinel ingests events over REST, learns a rolling baseline for every `entityId|metric` pair, and raises a severity graded alert when a value deviates sharply from normal. Alerts are stored durably in PostgreSQL and served through a REST API.

> **Built around the hard parts of event driven backends:** partition aware ordering, stateful stream processing with exactly once semantics, constant memory online statistics, idempotent persistence and built in observability. Key decisions are recorded as [ADRs](docs/).

## Contents

- [Architecture](#architecture)
- [Life of an event](#life-of-an-event)
- [Key design points](#key-design-points)
- [Delivery guarantees](#delivery-guarantees)
- [Tech stack](#tech-stack)
- [Detection model](#detection-model)
- [Quick start](#quick-start)
- [API reference](#api-reference)
- [Failure scenarios](#failure-scenarios)
- [Engineering decisions](#engineering-decisions)
- [Contributing](#contributing)
- [Repository layout](#repository-layout)
- [License](#license)

---

## Architecture

<p align="center">
  <img src="docs/images/architecture.svg" alt="Sentinel architecture: REST ingestion, Kafka, Kafka Streams detector with RocksDB state, alerts topic, PostgreSQL and Alert API" width="760">
</p>

**Reading the diagram**

- **Solid arrows** carry data. **Dashed arrows** are supporting flows (state backup, metrics, optional cache).
- The Streams topology and the PostgreSQL sink are decoupled by the `sentinel.alerts` topic. Detection keeps running even if the database is down, and alerts are consumed once it returns.
- Detection state lives next to the processing (RocksDB) and is backed up to Kafka, so there is no remote database call per event.

## Life of an event

1. **Client sends the event.** `POST /v1/events` reaches the ingestion API, which validates it.
2. **Event is published to Kafka.** The API produces it to `sentinel.events` with `entityId` as the key and replies `202 Accepted` right away. It does not wait for detection.
3. **Detector consumes it.** All events for one entity sit on the same partition, so they are processed in order.
4. **Baseline is read.** The detector loads the running mean and variance for that `entityId|metric` from the `baseline-store`.
5. **Event is scored.** It computes the z-score, unless the key is still in its warm-up phase.
6. **Baseline is updated.** The new value is folded into the mean and variance. This happens for every event, anomalous or not.
7. **Anomalies become alerts.** If z is 3 or higher, an alert with a severity is published to `sentinel.alerts`. Normal events produce no alert.
8. **Alert is stored.** The alert consumer upserts it into PostgreSQL by `alert_id`, so a redelivered alert never creates a duplicate row.
9. **Client reads alerts.** `GET /v1/alerts` and `GET /v1/alerts/severity/{severity}` read from PostgreSQL.

---

## Key design points

**1. Partition aware ordering.** Events are keyed by `entityId`, so all events for one entity land on the same Kafka partition. That preserves the ordering the per entity baseline needs while still scaling across partitions.

**2. Constant memory stateful processing.** The detector keeps a persistent Kafka Streams state store per `entityId|metric` key. Welford's online algorithm updates mean and variance in O(1) memory per key, so no event history is retained.

**3. Exactly once, with documented boundaries.** Kafka Streams runs with `exactly_once_v2`. The PostgreSQL sink is independently idempotent (keyed by `alert_id`) and is *not* part of the Kafka transaction. See [ADR-002](docs/adr-002-state-management.md).

**4. Durable, queryable alerts.** Alerts are not held in memory. A Kafka consumer persists them to PostgreSQL with composite indexes designed for the recent alerts and severity queries.

**5. Observable by design.** Micrometer tracks ingestion volume, anomaly rate, critical anomaly rate and detection latency. Prometheus scrapes them and Grafana visualises them.

## Delivery guarantees

| Stage | Guarantee | How |
| --- | --- | --- |
| Ingestion to Kafka | No duplicates from producer retries | Producer idempotence enabled |
| Kafka to detector to alerts topic | Exactly once | Kafka Streams `exactly_once_v2` |
| Alerts topic to PostgreSQL | Effectively once | At least once consumption plus idempotent write on `alert_id` |
| Detector restart | State and offsets recovered | RocksDB store restored from the changelog topic |

The PostgreSQL step is deliberately outside the Kafka transaction, so the guarantee there comes from idempotent writes rather than from Kafka.

---

## Tech stack

| Layer | Technology |
| --- | --- |
| Language | Java 21 |
| API | Spring Boot 3.4, REST, Bean Validation |
| Streaming | Apache Kafka, Kafka Streams |
| State management | Kafka Streams persistent state store (RocksDB) |
| Database | PostgreSQL 16, Spring Data JPA |
| Cache / extension | Redis 7 |
| Observability | Micrometer, Prometheus, Grafana, Spring Boot Actuator |
| Testing | JUnit 5, Spring Kafka Test, Testcontainers |
| Load testing | k6 |
| Runtime | Docker Compose, Kubernetes manifests |

---

## Detection model

Sentinel uses a simple, explainable approach: **online z-score detection** with Welford's algorithm.

After a configurable warm-up phase (default: 30 events per `entityId|metric` key), each value is scored:

```text
z = |x − μ| / σ
```

where `x` is the new value, `μ` the running mean and `σ` the running standard deviation.

| z-score | Severity |
| --- | --- |
| ≥ 3.0 | `LOW` |
| ≥ 4.0 | `MEDIUM` |
| ≥ 5.0 | `HIGH` |
| ≥ 6.0 | `CRITICAL` |

The detector is intentionally simple and easy to reason about. The [roadmap](docs/roadmap.md) covers future options such as EWMA, MAD and seasonality aware baselines.

---

## Quick start

### Prerequisites

- Docker and Docker Compose
- Java 21
- Maven 3.9+

### 1. Start Sentinel

```bash
git clone https://github.com/taniyanandi/sentinel.git
cd sentinel
```

Choose one of the following run modes.

#### Run everything in Docker

Build the application JAR before building the Docker image:

```bash
mvn clean verify
docker compose up -d --build
```

#### Run Sentinel locally

If the Sentinel container is already running, stop it before starting
the local application:

```bash
docker compose stop sentinel
```

Start the infrastructure services without the Sentinel container:

```bash
docker compose up -d kafka postgres redis prometheus grafana
mvn spring-boot:run
```

Kafka advertises `kafka:9092` to Docker clients and `localhost:9092`
to local clients through separate listeners.

Both run modes use port 8080 for Sentinel, so run only one at a time.

### 2. Ingest an event

```bash
curl -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId": "evt-1001",
    "source": "payment-service",
    "entityId": "merchant-42",
    "metric": "latency_ms",
    "value": 125.4,
    "timestamp": "2026-09-07T10:00:00Z",
    "attributes": {"region": "ap-south-1", "service": "checkout"}
  }'
```

### 3. Query alerts

```bash
# Recent alerts
curl "http://localhost:8080/v1/alerts?limit=20"

# Critical alerts only
curl "http://localhost:8080/v1/alerts/severity/CRITICAL?limit=20"
```

> **No alerts at first?** That is expected. Each `entityId|metric` pair needs its warm-up events before anything can be flagged.

### Dashboards and metrics

| Service | URL |
| --- | --- |
| Swagger UI | http://localhost:8080/swagger-ui.html |
| Health check | http://localhost:8080/actuator/health |
| Prometheus metrics | http://localhost:8080/actuator/prometheus |
| Prometheus UI | http://localhost:9090 |
| Grafana | http://localhost:3000 |

### Load test

```bash
k6 run loadtest/sentinel.js   # 100 events/sec for 60s
```

### Run tests

```bash
mvn test   # requires Docker for Testcontainers
```

More build details are in [BUILDING.md](BUILDING.md).

---

## API reference

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/v1/events` | Ingest a telemetry event. Returns `202 Accepted`. |
| `GET` | `/v1/alerts` | Recent alerts, newest first. `?limit=N` (max 200) |
| `GET` | `/v1/alerts/severity/{severity}` | Filter by `LOW`, `MEDIUM`, `HIGH` or `CRITICAL` |
| `GET` | `/actuator/health` | Liveness and readiness probes |
| `GET` | `/actuator/prometheus` | Prometheus scrape endpoint |

**Event fields**

| Field | Description |
| --- | --- |
| `eventId` | Unique ID of the event |
| `source` | System that produced the event |
| `entityId` | What is being measured (merchant, host, service). Used as the Kafka partition key |
| `metric` | Name of the measurement, for example `latency_ms` |
| `value` | Numeric value |
| `timestamp` | ISO 8601 event time |
| `attributes` | Optional key/value metadata |

---

## Failure scenarios

[docs/chaos-test-plan.md](docs/chaos-test-plan.md) contains a repeatable demo:

| Scenario | What to do | What to verify |
| --- | --- | --- |
| Detector restart | Kill and restart the process | State store restored, offsets recovered, processing resumes |
| PostgreSQL outage | Stop Postgres mid stream | Alert consumption retries, nothing lost, catch up after restart |
| Duplicate delivery | Replay the same alert | No duplicate rows in the database |
| Broker interruption | Stop Kafka, then restart it | Producer errors while down, reconnection after restart |

---

## Engineering decisions

| Decision | Rationale |
| --- | --- |
| Kafka over synchronous calls | Absorbs bursts and decouples ingestion from detector availability |
| Entity key partitioning | Preserves per entity ordering while enabling horizontal parallelism |
| Kafka Streams state store | Local state with changelog recovery, no remote call per event |
| PostgreSQL alert sink | Durable, queryable history independent of stream processing state |
| Explainable z-score | Deterministic and easy to reason about |
| Fail soft extensions | Future advanced components enrich the pipeline and never become hard dependencies |

Full reasoning in [ADR-001](docs/adr-001-partitioning.md) and [ADR-002](docs/adr-002-state-management.md).

---

## Contributing

Contributions are welcome, including for Hacktoberfest 2026. Browse the [good first issues](https://github.com/taniyanandi/sentinel/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22). Each one is scoped, has clear acceptance criteria and points to the files to change.

| Issue | Stack | Difficulty |
| --- | --- | --- |
| Add `GET /v1/alerts/{alertId}` to fetch a single alert | Java, Spring Boot | 🟢 Beginner |
| Add alert count endpoint by entity ID | Java, Spring Data | 🟢 Beginner |
| Add Bean Validation annotations to `EventRequest` fields | Java, Validation | 🟢 Beginner |
| Add OpenAPI annotations to all controllers | Java, Documentation | 🟢 Beginner |
| Create a Postman or Bruno API collection | Documentation | 🟢 Beginner |
| Add a Grafana dashboard JSON for Sentinel metrics | Grafana, Observability | 🟢 Beginner |
| Add event count and throughput endpoint | Java, Metrics | 🟡 Intermediate |
| Add correlation ID filter for request tracing | Java, SLF4J MDC | 🟡 Intermediate |
| Add dead letter topic for events that fail detection | Java, Kafka | 🟡 Intermediate |
| Add EWMA detector as an alternative to z-score | Java, Statistics | 🟡 Intermediate |
| Add alert suppression / cooldown per entity and metric | Java, Kafka Streams | 🔴 Advanced |

**First time contributing to open source?** Start with a 🟢 Beginner issue and comment on it to claim it before you begin. See [CONTRIBUTING.md](CONTRIBUTING.md) for the workflow.

---

## Repository layout

```text
sentinel/
├── src/main/java/com/taniyanandi/sentinel/
│   ├── api/            REST controllers and request DTOs
│   ├── admin/          Health and service metadata
│   ├── config/         Kafka and Redis configuration
│   ├── domain/         Immutable event and alert records
│   ├── metrics/        Micrometer instrumentation
│   ├── persistence/    PostgreSQL JPA alert store
│   ├── service/        Ingestion, anomaly detection, alert persistence
│   └── stream/         Kafka Streams topology
├── docs/
│   ├── architecture.md
│   ├── adr-001-partitioning.md
│   ├── adr-002-state-management.md
│   ├── chaos-test-plan.md
│   └── roadmap.md
├── loadtest/           k6 load test scenarios
├── infra/              Prometheus config
├── deploy/k8s/         Kubernetes manifests
├── .github/workflows/  CI workflows
├── docker-compose.yml
├── Dockerfile
└── pom.xml
```

---
