# Trino Local Setup

Local Trino environment backed by an **Iceberg** data lake, completely containerised.

The current validation has three parts:

1. Build the Wayang Trino platform and run the shared JDBC SQL-generation tests.
2. Run the Wayang Trino operator tests against the live local stack.
3. Run standalone JDBC integration tests against the local Trino, Iceberg, and MinIO stack.

Run the commands below from the repository root. Java 17 and Docker with
Docker Compose are required; Maven is provided by the repository wrapper.

The Trino cost-profiling branch is named `feature/trino-cost-profiling`:

```bash
git checkout feature/trino-cost-profiling
```

## Command Conventions

Use the `bash` blocks on macOS/Linux terminals. Use the `powershell` blocks on
Windows PowerShell from the repository root. Docker Compose commands are the
same on both platforms.

## Stack

| Component | Image | Port | Role |
|-----------|-------|------|------|
| **Trino** | `trinodb/trino:435` | 8080 | SQL query engine |
| **Hive Metastore** | `naushadh/hive-metastore:latest` | 9083 | Iceberg table catalog (Thrift) |
| **PostgreSQL** | `postgres:15-alpine` | 5432 | HMS metadata backing store |
| **MinIO** | `minio/minio:latest` | 9000 / 9001 | S3-compatible object storage |

HMS is the battle-tested Iceberg catalog for Trino. Parquet data files are written by Trino directly to MinIO; HMS only stores schema/table metadata.

## Directory Layout

```
trino-setup/
|-- docker-compose.yml          # Full stack definition
|-- trino/
|   |-- config.properties       # Trino node config
|   `-- catalog/
|       |-- iceberg.properties  # Iceberg via HMS + MinIO
|       `-- tpch.properties     # Built-in TPC-H (no storage needed)
|-- scripts/
|   |-- init.sql                # Creates iceberg.sales.orders + sample rows
|   `-- run-init.sh             # Helper: waits for Trino then runs init.sql
|-- pom.xml                     # Standalone Maven project (Java 17)
`-- src/test/java/.../
    `-- TrinoIntegrationTest.java   # JUnit 5 integration tests
```

## 1. Test the Wayang Trino Platform

Build the Trino platform and its required modules:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-trino -am -DskipTests -Drat.skip=true test
```

On PowerShell:

```powershell
.\mvnw.cmd --% -Pskip-prerequisite-check -pl wayang-platforms/wayang-trino -am -DskipTests -Drat.skip=true test
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
Wayang Platform Trino ... SUCCESS
Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
```

## 2. Test Against the Local Trino Stack

### 1. Start the stack

```bash
docker compose -f trino-setup/docker-compose.yml up -d
```

Wait ~30 seconds for all services to become healthy. Check with:

```bash
docker compose -f trino-setup/docker-compose.yml ps
# or watch the Trino UI at http://localhost:8080
```

### 2. Run the Wayang Trino operator tests

`TrinoOperatorsIT` exercises the Wayang Trino implementation against the live
Trino stack. It checks `TableSource`, `Filter`, `Projection`, `Join`,
`GlobalReduce`, `ReduceBy`, `Sort`, and `TableSink`, and confirms that the
expected SQL reached Trino. The standalone join test now runs a full Wayang
plan and normalizes both possible join result shapes before collecting records:
logical joins can produce `Tuple2<Record, Record>`, while pushed-down JDBC joins
can return a flat `Record`.

The suite is self-contained: it creates `iceberg.wayang_it`, scales its test
data to 120,000 rows so the optimizer selects SQL pushdown, and drops its test
tables afterward. It does not require `scripts/init.sql`. The suite also
contains five JavaPlanBuilder `readTable` combination tests that cover filter,
projection, global reduce, reduce-by plus sort, table sink, and join through
the public API.

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-trino -am \
  -Dtest=TrinoOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false \
  -DfailIfNoTests=false -Drat.skip=true -Dlicense.skip=true test
```

On PowerShell:

```powershell
.\mvnw.cmd --% -Pskip-prerequisite-check -pl wayang-platforms/wayang-trino -am -Dtest=TrinoOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false -Drat.skip=true -Dlicense.skip=true test
```

Expected result:

```text
Tests run: 13, Failures: 0, Errors: 0, Skipped: 0
```

Verified on June 18, 2026 against the local Docker stack with the full-plan
join test and all five JavaPlanBuilder combination tests enabled.

If Trino is unreachable, these tests are skipped instead of failed. A result
with skipped tests does not confirm that the operators work.

### 3. Load sample Iceberg data

```bash
bash trino-setup/scripts/run-init.sh
```

On PowerShell:

```powershell
Get-Content -Raw trino-setup/scripts/init.sql | docker exec -i trino trino --server http://localhost:8080 --user admin
```

This creates the schema `iceberg.sales` and inserts 20 sample orders into
`iceberg.sales.orders` (Parquet files on MinIO).

### 4. Run the standalone stack integration tests

```bash
./mvnw -f trino-setup/pom.xml -Pintegration -Dtest=TrinoIntegrationTest test
```

On PowerShell:

```powershell
.\mvnw.cmd --% -f trino-setup/pom.xml -Pintegration -Dtest=TrinoIntegrationTest test
```

Tests are skipped by default (no `-Pintegration`) to avoid requiring Docker in CI.
These tests validate the stack and direct JDBC queries independently of the
Wayang operator implementation.

Expected result:

```text
Tests run: 10, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

### 5. Manual exploration

Open the **Trino UI**: http://localhost:8080

Or connect via the Trino CLI inside the container:

```bash
docker exec -it trino trino --catalog iceberg --schema sales
```

```sql
-- TPC-H built-in data (no init.sql needed)
SELECT * FROM tpch.tiny.orders LIMIT 5;

-- Iceberg table
SELECT region, SUM(amount) FROM iceberg.sales.orders GROUP BY region;

-- Iceberg file metadata
SELECT * FROM iceberg.sales."orders$files";

-- Iceberg history
SELECT * FROM iceberg.sales."orders$history";
```

**MinIO console**: http://localhost:9001 (login: `minioadmin` / `minioadmin`)
Look for Parquet files under `warehouse/sales/orders/`.

### 6. Tear down

```bash
docker compose -f trino-setup/docker-compose.yml down -v
```

The `-v` option removes volumes and clears the local MinIO and PostgreSQL data.

## Test Coverage

### Wayang operator integration tests

| Test | What it checks |
|------|----------------|
| `tableSource` | Full table scan through `TrinoTableSource` |
| `filter` | Wayang `FilterOperator` and SQL `WHERE` pushdown |
| `projection` | Column projection pushed into the Trino query |
| `join` | Full Wayang join plan with normalization before the collecting sink |
| `globalReduce` | Global aggregation such as `SUM` |
| `reduceBy` | Grouped aggregation and SQL `GROUP BY` |
| `sort` | Wayang sort and SQL `ORDER BY` |
| `tableSink` | Filtered result written with `CREATE TABLE AS` |
| `javaPlanBuilderReadTableFilterProjection` | `readTable -> filter -> projection -> collect` |
| `javaPlanBuilderReadTableFilterGlobalReduce` | `readTable -> filter -> globalReduce -> collect` |
| `javaPlanBuilderReadTableReduceBySort` | `readTable -> reduceByKey -> sort -> collect` |
| `javaPlanBuilderReadTableFilterProjectionTableSink` | `readTable -> filter -> projection -> writeTable` |
| `javaPlanBuilderReadTableJoin` | `readTable + readTable -> join -> collect` |

### Standalone stack integration tests

| Test | What it checks |
|------|----------------|
| `testConnectivity` | `SELECT 1`, JDBC connection works |
| `testTpchConnector` | TPC-H built-in connector, no storage needed |
| `testTpchTopOrders` | ORDER BY + LIMIT on TPC-H |
| `testIcebergSchemaVisible` | Schema created by `init.sql` is visible |
| `testIcebergSelectAll` | Full table scan, 20 rows |
| `testIcebergFilterByRegion` | WHERE pushdown on string column |
| `testIcebergAggregate` | GROUP BY + SUM aggregation |
| `testIcebergFilterByAmount` | WHERE pushdown on double column |
| `testIcebergProjection` | SELECT subset of columns |
| `testIcebergFilesMetadata` | `$files` system table, confirms Parquet on MinIO |

## Cost Profiling

Follow the shared cost-profiling guide in
[`guides/cost-profiling.md`](../guides/cost-profiling.md). This setup guide
only covers the Trino stack itself.

Trino-specific profiling values:

| Item | Value |
|------|-------|
| Maven module | `wayang-platforms/wayang-trino` |
| Profiling test | `TrinoCostPilotIT` |
| Property prefix | `trino.profile.*` |
| Default output directory | `target/cost-profiling/trino` |
| Learned parameters file | `wayang-platforms/wayang-trino/src/main/resources/wayang-trino-defaults.properties` |

## Environment Variables

Override defaults if running Trino on a different host/port:

```bash
TRINO_HOST=my-trino-host TRINO_PORT=8080 ./mvnw -f trino-setup/pom.xml -Pintegration -Dtest=TrinoIntegrationTest test
```

On PowerShell:

```powershell
$env:TRINO_HOST="my-trino-host"
$env:TRINO_PORT="8080"
.\mvnw.cmd --% -f trino-setup/pom.xml -Pintegration -Dtest=TrinoIntegrationTest test
Remove-Item Env:TRINO_HOST, Env:TRINO_PORT
```
