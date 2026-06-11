# BigQuery Local Setup

Local BigQuery emulator and validation instructions for the Wayang BigQuery
platform.

The current validation has two parts:

1. Build the Wayang BigQuery platform and run the shared JDBC SQL-generation tests.
2. Run BigQuery-compatible SQL tests against the local emulator.

Run the commands below from the repository root. Java and Docker with Docker
Compose are required; Maven is provided by the repository wrapper.

```bash
git checkout wayang-bigquery
```

## Stack

| Component | Image | Port | Role |
|-----------|-------|------|------|
| **BigQuery Emulator** | `ghcr.io/goccy/bigquery-emulator:0.6.6` | 9050 (HTTP) / 9060 (gRPC) | BigQuery-compatible SQL engine |

Single container. Data is seeded from `data.yaml` on startup and lives in memory.

## Directory Layout

```
bigquery-setup/
|-- docker-compose.yml          # Emulator container
|-- data.yaml                   # Seed data (test-project.sales.orders)
|-- pom.xml                     # Standalone Maven project
`-- src/test/java/.../
    `-- BigQueryEmulatorIT.java # JUnit 5 integration tests
```

## 1. Test the Wayang BigQuery Platform

Build the BigQuery platform and its required modules:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-bigquery -am -DskipTests -Drat.skip=true test
```

On PowerShell:

```powershell
.\mvnw.cmd --% -Pskip-prerequisite-check -pl wayang-platforms/wayang-bigquery -am -DskipTests -Drat.skip=true test
```

Then run the shared JDBC SQL-generation tests:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-jdbc-template -am -Dtest=JdbcExecutorTest -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false -Drat.skip=true test
```

On PowerShell:

```powershell
.\mvnw.cmd --% -Pskip-prerequisite-check -pl wayang-platforms/wayang-jdbc-template -am -Dtest=JdbcExecutorTest -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false -Drat.skip=true test
```

Expected result:

```text
Wayang Platform BigQuery ... SUCCESS
Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
```

## 2. Test the Local BigQuery Emulator

### 1. Start the emulator

```bash
docker compose -f bigquery-setup/docker-compose.yml up -d
```

The emulator starts in ~2 seconds. Data from `data.yaml` is loaded automatically.

### 2. Run integration tests

```bash
./mvnw -f bigquery-setup/pom.xml -Dtest=BigQueryEmulatorIT test
```

On PowerShell:

```powershell
.\mvnw.cmd --% -f bigquery-setup/pom.xml -Dtest=BigQueryEmulatorIT test
```

The successful result must show that no tests were skipped:

```text
Tests run: 7, Failures: 0, Errors: 0, Skipped: 0
```

If the emulator is unavailable, Maven can still print `BUILD SUCCESS` while
showing `Skipped: 7`. That does not count as a successful emulator test.

### 3. Manual exploration

Query via curl:

```bash
curl -s -X POST \
  "http://localhost:9050/bigquery/v2/projects/test-project/queries" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM sales.orders LIMIT 5", "useLegacySql": false}' \
  | python3 -m json.tool
```

### 4. Tear down

```bash
docker compose -f bigquery-setup/docker-compose.yml down
```

## Test Coverage

| Test | What it checks |
|------|----------------|
| `testDatasetVisible` | `sales` dataset exists |
| `testFullScan` | Full table scan, 10 rows |
| `testFilterByRegion` | `WHERE region = 'APAC'` |
| `testFilterByAmount` | `WHERE amount > 1000` |
| `testAggregation` | `GROUP BY region` + `SUM(amount)` |
| `testProjection` | `SELECT region, product LIMIT 5` |
| `testCount` | `SELECT count(*)`, used by Wayang for cardinality estimation |

## Environment Variables

```bash
BIGQUERY_HOST=http://localhost:9050 ./mvnw -f bigquery-setup/pom.xml -Dtest=BigQueryEmulatorIT test
```

## Notes

- Tests use `google-cloud-bigquery` client library (REST-based, no JDBC).
- The client connects with `NoCredentials`; no GCP account is needed.
- The BigQuery JDBC driver (`google-cloud-bigquery-jdbc`) requires OAuth even against the emulator, so JDBC-based tests are not included yet.
- These tests do not prove end-to-end Wayang-to-Google-BigQuery JDBC execution. That requires a real GCP project, credentials, and JDBC URL.
