# DuckDB Local Setup

Local DuckDB setup for the Wayang DuckDB platform.

DuckDB is embedded: there is no coordinator or long-running database service to
start. The Wayang platform connects directly to a DuckDB database file through
the DuckDB JDBC driver. For a Trino-like reproducible local workflow, this setup
uses the official DuckDB CLI Docker image to create and inspect a local
`data/wayang.duckdb` file, then runs the Wayang operator tests against that
same file.

Run the commands below from the repository root. Java 17 and Docker with Docker
Compose are required; Maven is provided by the repository wrapper.

## Stack

| Component | Image | Role |
|-----------|-------|------|
| DuckDB CLI | `duckdb/duckdb:1.5.5` | Creates and inspects the local database file |
| DuckDB JDBC | `org.duckdb:duckdb_jdbc:1.5.5.1` | Runs Wayang plans against that file |

The Docker service is a one-shot CLI container. It exits after running the SQL
command; that is expected.

## 1. Create The Local DuckDB File

```bash
mkdir -p platforms-setup-guides/duckdb-setup/data
docker compose -f platforms-setup-guides/duckdb-setup/docker-compose.yml run --rm duckdb
```

On PowerShell:

```powershell
New-Item -ItemType Directory -Force platforms-setup-guides/duckdb-setup/data
docker compose -f platforms-setup-guides/duckdb-setup/docker-compose.yml run --rm duckdb
```

Expected output includes grouped totals for `APAC`, `AMER`, and `EMEA`.

## 2. Inspect The File Directly

```bash
docker run --rm -i \
  -v "$PWD/platforms-setup-guides/duckdb-setup:/workspace" \
  duckdb/duckdb:1.5.5 \
  duckdb /workspace/data/wayang.duckdb < platforms-setup-guides/duckdb-setup/scripts/check.sql
```

On PowerShell:

```powershell
Get-Content -Raw platforms-setup-guides/duckdb-setup/scripts/check.sql |
  docker run --rm -i -v "${PWD}/platforms-setup-guides/duckdb-setup:/workspace" duckdb/duckdb:1.5.5 duckdb /workspace/data/wayang.duckdb
```

## 3. Run Wayang Tests Against The Docker-Created File

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-duckdb -am \
  -Dtest=DuckDBOperatorsIT \
  -Dduckdb.url=jdbc:duckdb:platforms-setup-guides/duckdb-setup/data/wayang.duckdb \
  -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false \
  -Drat.skip=true -Dlicense.skip=true test
```

On PowerShell:

```powershell
.\mvnw.cmd --% -Pskip-prerequisite-check -pl wayang-platforms/wayang-duckdb -am -Dtest=DuckDBOperatorsIT -Dduckdb.url=jdbc:duckdb:platforms-setup-guides/duckdb-setup/data/wayang.duckdb -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false -Drat.skip=true -Dlicense.skip=true test
```

Expected result:

```text
Tests run: 15, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

`DuckDBOperatorsIT` recreates the `wayang_it` fixtures before it runs, so the
test is deterministic even if the local database file already exists.

## 4. Run Parquet And GCS Tests

`DuckDBParquetSourceIT` creates a local Parquet file, reads it through DuckDB
auto-created `read_parquet(...)` views, checks URI-to-relation mappings, and
tries a public GCS Parquet smoke through DuckDB `httpfs`.

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-duckdb -am \
  -Dtest=DuckDBParquetSourceIT \
  -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false \
  -Drat.skip=true -Dlicense.skip=true test
```

On PowerShell:

```powershell
.\mvnw.cmd --% -Pskip-prerequisite-check -pl wayang-platforms/wayang-duckdb -am -Dtest=DuckDBParquetSourceIT -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false -Drat.skip=true -Dlicense.skip=true test
```

Override the GCS object with `-Dduckdb.gcs.parquet.uri=gs://bucket/path/file.parquet`.
If DuckDB cannot install/load `httpfs` or reach the object, the GCS smoke is
skipped; the local Parquet tests still run.

## 5. Run Cost Profiling Smoke

For a fast local check, run two profiling plans over two small cardinalities:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-duckdb -am \
  -Dtest=DuckDBCostPilotIT#runPilot \
  -Dduckdb.profile.rowCounts=100,1000 \
  -Dduckdb.profile.plans=S01,S02 \
  -Dduckdb.profile.repetitions=2 \
  -Drat.skip=true -Dlicense.skip=true test
```

The Trino Week8-style reference pilot is S01-S13 over four cardinalities and
six repetitions, producing 312 Wayang executions:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-duckdb -am \
  -Dtest=DuckDBCostPilotIT#runPilot \
  -Dduckdb.profile.rowCounts=10000,50000,100000,250000 \
  -Dduckdb.profile.plans=S01,S02,S03,S04,S05,S06,S07,S08,S09,S10,S11,S12,S13 \
  -Dduckdb.profile.repetitions=6 \
  -Drat.skip=true -Dlicense.skip=true test
```

Outputs are written under `wayang-platforms/wayang-duckdb/target/cost-profiling/`.
Run the GA optimizer outside Surefire, matching the Trino profiling workflow:

```powershell
.\platforms-setup-guides\duckdb-setup\scripts\run-duckdb-ga.ps1
```

The script writes
`wayang-platforms/wayang-duckdb/target/cost-profiling/duckdb/learned-duckdb-relaxed.properties`.
S14-S16 are implemented for optional expanded runs, but the checked-in reference
parameters are learned from S01-S13. The GA optimizer is stochastic, so repeated
runs over the same execution log can produce slightly different coefficients.

## 6. Run The Standalone Setup Integration Tests

The setup directory includes a small Maven project that validates the local
DuckDB database independently of Wayang. Tests are skipped by default; enable
them with `-Pintegration`.

```bash
./mvnw -f platforms-setup-guides/duckdb-setup/pom.xml \
  -Pintegration -Dtest=DuckDBIntegrationTest test
```

On PowerShell:

```powershell
.\mvnw.cmd --% -f platforms-setup-guides/duckdb-setup/pom.xml -Pintegration -Dtest=DuckDBIntegrationTest test
```

Expected result:

```text
Tests run: 10, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Override the database file:

```bash
DUCKDB_JDBC_URL=jdbc:duckdb:/tmp/wayang.duckdb ./mvnw -f platforms-setup-guides/duckdb-setup/pom.xml -Pintegration -Dtest=DuckDBIntegrationTest test
```

On PowerShell:

```powershell
$env:DUCKDB_JDBC_URL="jdbc:duckdb:C:/tmp/wayang.duckdb"
.\mvnw.cmd --% -f platforms-setup-guides/duckdb-setup/pom.xml -Pintegration -Dtest=DuckDBIntegrationTest test
Remove-Item Env:DUCKDB_JDBC_URL
```

## 7. Run The Walkthrough Demo

The optional demo script creates the local DuckDB file, runs the Wayang DuckDB
operator tests against it, runs the standalone JDBC integration tests, and
executes `org.apache.wayang.duckdb.DuckDBDemo`.

```bash
bash platforms-setup-guides/duckdb-setup/demo.sh
```

Set `WAYANG_DEMO_AUTO=true` to skip the interactive pauses.

## 8. Clean Up

```bash
rm -f platforms-setup-guides/duckdb-setup/data/wayang.duckdb*
```

On PowerShell:

```powershell
Remove-Item platforms-setup-guides/duckdb-setup/data/wayang.duckdb* -Force
```
