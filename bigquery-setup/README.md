# BigQuery Local Setup

Local BigQuery emulator and validation instructions for the Wayang BigQuery
platform.

The local validation path has two parts and does not require a GCP account:

1. Build the Wayang BigQuery platform and run the shared JDBC SQL-generation tests.
2. Run BigQuery-compatible SQL tests against the local emulator.

There is also an optional real-BigQuery validation path:

3. Run the Wayang BigQuery operator tests through JDBC against real BigQuery.

Run the commands below from the repository root. Java 17 and Docker with Docker
Compose are required for the emulator tests. A GCP project and service-account
key, plus the `gcloud` SDK, are required only for the optional real-BigQuery
operator tests. Maven is provided by the repository wrapper.

## Command Conventions

Use the `bash` blocks on macOS/Linux terminals. Use the `powershell` blocks on
Windows PowerShell from the repository root. Docker Compose commands are the
same on both platforms. The `gcloud` commands also work on Windows; either run
each command on one line or replace Bash line-continuation backslashes with
PowerShell backticks.

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

wayang-platforms/wayang-bigquery/src/test/java/.../
`-- BigQueryOperatorsIT.java    # Wayang operator tests against real BigQuery
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

## 3. Optional: Test the Wayang Operators Against Real BigQuery

`BigQueryOperatorsIT` uses the BigQuery JDBC driver and cannot run against the
local emulator. It requires a real GCP project and a service-account JSON key.

The test setup creates its own fixture tables in a configurable dataset
(`wayang_it` by default): `orders`, `regions`, and `operator_result`. The tests
issue `SELECT`, `CREATE TABLE AS`, and `DROP` statements, then remove those
tables during cleanup.

### 1. Enable BigQuery and create a service account

Replace `YOUR_PROJECT_ID` in the following commands:

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud services enable bigquery.googleapis.com

gcloud iam service-accounts create wayang-bq \
  --display-name="Wayang BigQuery IT"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:wayang-bq@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:wayang-bq@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud iam service-accounts keys create "$HOME/wayang-bq-key.json" \
  --iam-account="wayang-bq@YOUR_PROJECT_ID.iam.gserviceaccount.com"
```

On Windows PowerShell, the same setup can be run as:

```powershell
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud services enable bigquery.googleapis.com
gcloud iam service-accounts create wayang-bq --display-name="Wayang BigQuery IT"
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID --member="serviceAccount:wayang-bq@YOUR_PROJECT_ID.iam.gserviceaccount.com" --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID --member="serviceAccount:wayang-bq@YOUR_PROJECT_ID.iam.gserviceaccount.com" --role="roles/bigquery.dataEditor"
gcloud iam service-accounts keys create "$HOME\wayang-bq-key.json" --iam-account="wayang-bq@YOUR_PROJECT_ID.iam.gserviceaccount.com"
```

The service account needs `jobUser` to run queries and `dataEditor` to create,
read, and drop the test dataset tables.

### 2. Choose the test dataset

The test creates the dataset if it does not exist. By default it uses
`wayang_it`; override it with `-Dbigquery.dataset=DATASET_ID` or
`BIGQUERY_DATASET` if the project needs a different dataset name.

### 3. Run the operator tests

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-bigquery -am \
  -Dtest=BigQueryOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false \
  -DfailIfNoTests=false \
  -Dbigquery.project=YOUR_PROJECT_ID \
  -Dbigquery.saEmail=wayang-bq@YOUR_PROJECT_ID.iam.gserviceaccount.com \
  -Dbigquery.keyPath="$HOME/wayang-bq-key.json" \
  -Dbigquery.location=US \
  -Dbigquery.dataset=wayang_it \
  -Drat.skip=true -Dlicense.skip=true test
```

On PowerShell:

```powershell
.\mvnw.cmd --% -Pskip-prerequisite-check -pl wayang-platforms/wayang-bigquery -am -Dtest=BigQueryOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false -Dbigquery.project=YOUR_PROJECT_ID -Dbigquery.saEmail=wayang-bq@YOUR_PROJECT_ID.iam.gserviceaccount.com -Dbigquery.keyPath=C:\path\to\wayang-bq-key.json -Dbigquery.location=US -Dbigquery.dataset=wayang_it -Drat.skip=true -Dlicense.skip=true test
```

System properties take precedence over the equivalent environment variables:

| System property | Environment variable | Default |
|-----------------|----------------------|---------|
| `bigquery.project` | `BIGQUERY_PROJECT` | `your-project` |
| `bigquery.saEmail` | `BIGQUERY_SA_EMAIL` | `wayang-bq@<project>.iam.gserviceaccount.com` |
| `bigquery.keyPath` | `BIGQUERY_KEY_PATH` | `$HOME/wayang-bq-key.json` |
| `bigquery.location` | `BIGQUERY_LOCATION` | `US` |
| `bigquery.dataset` | `BIGQUERY_DATASET` | `wayang_it` |

Successful real-BigQuery validation must show:

```text
Tests run: 13, Failures: 0, Errors: 0, Skipped: 0
```

If the browser uses a local proxy, pass the same proxy to both CLI tools and
the Maven test JVM. For example, with a proxy at `127.0.0.1:7890`, set
`HTTP_PROXY`/`HTTPS_PROXY` and use `JAVA_TOOL_OPTIONS` with
`-Dhttp.proxyHost`, `-Dhttp.proxyPort`, `-Dhttps.proxyHost`, and
`-Dhttps.proxyPort`.

On PowerShell:

```powershell
$env:HTTP_PROXY="http://127.0.0.1:7890"
$env:HTTPS_PROXY="http://127.0.0.1:7890"
$env:JAVA_TOOL_OPTIONS="-Dhttp.proxyHost=127.0.0.1 -Dhttp.proxyPort=7890 -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=7890"
.\mvnw.cmd --% -Pskip-prerequisite-check -pl wayang-platforms/wayang-bigquery -am -Dtest=BigQueryOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false -Dbigquery.project=YOUR_PROJECT_ID -Dbigquery.saEmail=wayang-bq@YOUR_PROJECT_ID.iam.gserviceaccount.com -Dbigquery.keyPath=C:\path\to\wayang-bq-key.json -Dbigquery.location=US -Dbigquery.dataset=wayang_it -Drat.skip=true -Dlicense.skip=true test
Remove-Item Env:HTTP_PROXY, Env:HTTPS_PROXY, Env:JAVA_TOOL_OPTIONS
```

If credentials or the project configuration are missing, Maven can still print
`BUILD SUCCESS` with `Skipped: 13`. That does not count as successful
real-BigQuery validation.

## 4. Optional: Re-run Cost Profiling

Follow the shared cost-profiling guide in
[`guides/cost-profiling.md`](../guides/cost-profiling.md). This setup guide
only covers the BigQuery emulator and real BigQuery validation setup. BigQuery
cost profiling uses `BigQueryCostPilotIT`, so it needs the same real-BigQuery
credentials as the optional operator tests above.

BigQuery-specific profiling values:

| Item | Value |
|------|-------|
| Maven module | `wayang-platforms/wayang-bigquery` |
| Profiling test | `BigQueryCostPilotIT` |
| Property prefix | `bigquery.profile.*` |
| Profiling dataset property | `bigquery.profile.dataset` |
| Default output directory | `target/cost-profiling/bigquery` |
| Learned parameters file | `wayang-platforms/wayang-bigquery/src/main/resources/wayang-bigquery-defaults.properties` |

## Test Coverage

### Local emulator tests

| Test | What it checks |
|------|----------------|
| `testDatasetVisible` | `sales` dataset exists |
| `testFullScan` | Full table scan, 10 rows |
| `testFilterByRegion` | `WHERE region = 'APAC'` |
| `testFilterByAmount` | `WHERE amount > 1000` |
| `testAggregation` | `GROUP BY region` + `SUM(amount)` |
| `testProjection` | `SELECT region, product LIMIT 5` |
| `testCount` | `SELECT count(*)`, used by Wayang for cardinality estimation |

### Real BigQuery operator tests

| Test | What it checks |
|------|----------------|
| `tableSource` | Full table scan through Wayang into a BigQuery sink table |
| `filter` | String filter pushdown |
| `projection` | Multi-column projection pushdown |
| `join` | Full Wayang join plan with normalization before the sink table |
| `globalReduce` | Global `SUM(amount)` |
| `reduceBy` | `SUM(amount) GROUP BY region` |
| `sort` | BigQuery sort operator SQL-clause contract |
| `tableSink` | `CREATE TABLE AS SELECT` and cleanup |
| `javaPlanBuilderReadTableFilterProjection` | `readTable -> filter -> projection -> writeTable` |
| `javaPlanBuilderReadTableFilterGlobalReduce` | `readTable -> filter -> globalReduce -> writeTable` |
| `javaPlanBuilderReadTableReduceBySort` | `readTable -> reduceByKey -> sort -> writeTable` |
| `javaPlanBuilderReadTableFilterProjectionTableSink` | `readTable -> filter -> projection -> writeTable` |
| `javaPlanBuilderReadTableJoin` | `readTable + readTable -> join -> writeTable` |

The combination tests use `.withTargetPlatform(BigQuery.platform())` so the
small 10-row fixture still exercises BigQuery SQL pushdown. The join test creates
and cleans up a temporary distinct-region lookup table.

## Emulator Environment Variable

```bash
BIGQUERY_HOST=http://localhost:9050 ./mvnw -f bigquery-setup/pom.xml -Dtest=BigQueryEmulatorIT test
```

On PowerShell:

```powershell
$env:BIGQUERY_HOST="http://localhost:9050"
.\mvnw.cmd --% -f bigquery-setup/pom.xml -Dtest=BigQueryEmulatorIT test
Remove-Item Env:BIGQUERY_HOST
```

## Notes

- Emulator tests use the `google-cloud-bigquery` client library (REST-based, no
  JDBC).
- The emulator client connects with `NoCredentials`; no GCP account is needed.
- The BigQuery JDBC driver (`google-cloud-bigquery-jdbc`) requires OAuth even
  against the emulator, so `BigQueryOperatorsIT` runs only against real
  BigQuery.
- Emulator tests validate SQL compatibility, but only `BigQueryOperatorsIT`
  validates end-to-end Wayang-to-BigQuery JDBC execution.
