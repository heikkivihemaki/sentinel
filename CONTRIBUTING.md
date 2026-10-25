# Contributing to Sentinel

Thanks for your interest in contributing! This guide gets you from zero to an open PR in under 10 minutes.

## Quick start

### Prerequisites

- Java 21
- Maven 3.9+
- Docker and Docker Compose (required for running the infrastructure locally)

### Fork and clone

```bash
# Fork via GitHub UI, then:
git clone https://github.com/<your-username>/sentinel.git
cd sentinel
```

### Start infrastructure

Start the infrastructure services without the Sentinel container:

```bash
docker compose up -d kafka postgres redis prometheus grafana
```

If the Sentinel container is already running, stop it before running
the application locally:

```bash
docker compose stop sentinel
```

### Run the app

```bash
mvn spring-boot:run
```

The local application connects to Kafka at `localhost:9092`.
The Sentinel container uses `kafka:9092` through a separate listener.

Check the application health:

```bash
curl -i http://localhost:8080/actuator/health
```

An HTTP 200 response with `"status":"UP"` confirms the health endpoint
is reporting healthy. It does not by itself verify event processing.

For running everything in Docker, see [BUILDING.md](BUILDING.md).

### Run the tests

```bash
mvn test
```

Tests use Testcontainers so they spin up a real PostgreSQL instance — all tests must pass before you open a PR.

---

## Picking an issue

Look for issues labelled **[`good first issue`](https://github.com/taniyanandi/sentinel/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)**. Each one:

- Points to the exact file(s) to change
- Has clear acceptance criteria
- Is scoped to a single, self-contained change

Comment on the issue to claim it before starting — so two people don't work on the same thing.

---

## Branch and commit conventions

```bash
# Branch off main
git checkout main
git pull origin main
git checkout -b feat/alert-by-id-endpoint    # or fix/validation-annotations

# Make your changes, then commit
git add <files>
git commit -m "feat: add GET /v1/alerts/{alertId} endpoint"
```

**Branch naming:** `feat/<short-description>`, `fix/<short-description>`, `test/<short-description>`, `docs/<short-description>`.

**Commit messages** follow [Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `test:`, `docs:`, `refactor:`.

---

## Opening a pull request

1. Push your branch: `git push origin <branch-name>`
2. Open a PR against `main` on the upstream repo
3. In the PR description, reference the issue: `Closes #<number>`
4. All tests must pass

A maintainer will review within a few days. For Hacktoberfest, make sure your PR is not a trivial change (docs typos don't count) — the issues labelled `hacktoberfest` are pre-approved as meaningful contributions.

---

## Code style

- Standard Java naming conventions
- Constructor injection (no `@Autowired` on fields)
- Records for immutable domain objects (see `TelemetryEvent.java`, `AnomalyAlert.java`)
- Spring Boot patterns already in the codebase — match the existing style
- New endpoints need: a controller method, a service method if there's logic, and ideally a test
- Configuration properties go in `application.yml` under the `sentinel:` namespace

---

## Architecture overview

Understanding the data flow helps with any contribution:

1. `POST /v1/events` → `EventController` validates and passes to `EventIngestionService`
2. `EventIngestionService` publishes to Kafka topic `sentinel.events` keyed by `entityId`
3. `EventStreamProcessor` (Kafka Streams) re-keys by `entityId|metric` and calls `AnomalyDetector`
4. `AnomalyDetector` evaluates against the baseline (Welford's algorithm in `DetectionState`)
5. Anomalies are written to `sentinel.alerts` topic
6. `AlertPersistenceListener` consumes alerts and persists to PostgreSQL (idempotent on `alert_id`)
7. `AlertController` serves query endpoints over the persisted alerts

Read [docs/architecture.md](docs/architecture.md) for the full picture.

---

## Need help?

Open a GitHub Discussion or leave a comment on the issue. Happy to answer questions about the architecture, Kafka Streams, or how any part of the system works.
