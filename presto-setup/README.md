# Presto Local Setup

Local PrestoDB environment using the built-in **memory** connector, completely
containerised.

The current validation has two parts:

1. Build the Wayang Presto platform and run the shared JDBC SQL-generation tests.
2. Run the Wayang Presto operator tests against the live local PrestoDB.

Run the commands below from the repository root. Java 17 and Docker with Docker
Compose are required; Maven is provided by the repository wrapper.

The Presto platform branch is named `wayang-presto`:

```bash
git checkout wayang-presto
```

## Stack

| Component | Image | Port | Role |
|-----------|-------|------|------|
| **PrestoDB** | `prestodb/presto:0.289` | 8081 | SQL engine and in-memory test storage |

The container listens on port `8080`; Docker exposes it as `8081` to avoid
clashing with the Trino setup. The `memory` connector needs no metastore,
database, or object storage. All tables disappear when the container stops.

## Directory Layout

```text
presto-setup/
|-- docker-compose.yml
|-- README.md
`-- etc/
    `-- catalog/
        `-- memory.properties

wayang-platforms/wayang-presto/src/test/java/.../
`-- PrestoOperatorsIT.java
```

## 1. Test the Wayang Presto Platform

Build the Presto platform and its required modules:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-presto -am \
  -DskipTests -Drat.skip=true test
```

On PowerShell:

```powershell
.\mvnw.cmd --% -Pskip-prerequisite-check -pl wayang-platforms/wayang-presto -am -DskipTests -Drat.skip=true test
```

Then run the shared JDBC SQL-generation tests:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-jdbc-template -am \
  -Dtest=JdbcExecutorTest -Dsurefire.failIfNoSpecifiedTests=false \
  -DfailIfNoTests=false -Drat.skip=true test
```

On PowerShell:

```powershell
.\mvnw.cmd --% -Pskip-prerequisite-check -pl wayang-platforms/wayang-jdbc-template -am -Dtest=JdbcExecutorTest -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false -Drat.skip=true test
```

Expected result:

```text
Wayang Platform Presto ... SUCCESS
Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
```

## 2. Test Against the Local Presto Stack

### 1. Start Presto

```bash
docker compose -f presto-setup/docker-compose.yml up -d --wait
```

Presto can take 20-60 seconds to accept queries. Confirm that it is healthy:

```bash
docker compose -f presto-setup/docker-compose.yml ps
```

The Presto web UI is available at <http://localhost:8081>.

### 2. Run the Wayang Presto operator tests

`PrestoOperatorsIT` exercises the Wayang Presto implementation against the live
container. It checks `TableSource`, `Filter`, `Projection`, `Join`,
`GlobalReduce`, `ReduceBy`, `Sort`, and `TableSink`, and confirms that the
expected SQL reached Presto through `system.runtime.queries`.

The standalone join test now runs as a full Wayang plan:
`PrestoTableSource + PrestoTableSource -> JoinOperator -> MapOperator -> sink`.
The normalization map accepts both logical `Tuple2<Record, Record>` output and
pushed-down JDBC flat `Record` output. The suite also includes five
`JavaPlanBuilder.readTable` combination plans. Together, they cover every
supported Presto operator through the public API.

The suite is self-contained. It creates `memory.wayang_it`, generates 120,000
rows so the optimizer selects SQL pushdown, runs the tests, and drops its tables
afterward.

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-presto -am \
  -Dtest=PrestoOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false \
  -DfailIfNoTests=false -Drat.skip=true -Dlicense.skip=true test
```

On PowerShell:

```powershell
.\mvnw.cmd --% -Pskip-prerequisite-check -pl wayang-platforms/wayang-presto -am -Dtest=PrestoOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false -Drat.skip=true -Dlicense.skip=true test
```

Successful validation must show:

```text
Tests run: 13, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

If Presto is unreachable, the tests are skipped instead of failed. A result
with skipped tests does not confirm that the operators work. Errors while
creating the test schema or tables are treated as real failures.

### Verified Result

On June 18, 2026, the suite completed successfully against the local PrestoDB
0.289 container, including the full-plan join validation:

```text
[PrestoOperatorsIT] Connected to Presto at jdbc:presto://localhost:8081/memory
Executed sql sink: CREATE TABLE memory.wayang_it.amer_orders AS SELECT ...
Tests run: 13, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

This verified the complete `Wayang -> Presto JDBC -> live PrestoDB` path,
including reads, SQL pushdown, join normalization, aggregation, sorting, and
`CREATE TABLE AS SELECT`.

### 3. Tear down

```bash
docker compose -f presto-setup/docker-compose.yml down
```

## Test Coverage

| Test | What it checks |
|------|----------------|
| `tableSource` | Full table scan through `PrestoTableSource` |
| `filter` | Wayang `FilterOperator` and SQL `WHERE` pushdown |
| `projection` | Column projection pushed into the Presto query |
| `join` | Full Wayang join plan with normalization before the collecting sink |
| `globalReduce` | Global aggregation such as `SUM` |
| `reduceBy` | Grouped aggregation and SQL `GROUP BY` |
| `sort` | Wayang sort and SQL `ORDER BY` |
| `tableSink` | Filtered result written with `CREATE TABLE AS` |
| `javaPlanBuilderReadTableFilterProjection` | Public API filter and projection combination |
| `javaPlanBuilderReadTableFilterGlobalReduce` | Public API filter and global aggregation combination |
| `javaPlanBuilderReadTableReduceBySort` | Public API grouped aggregation and sort combination |
| `javaPlanBuilderReadTableFilterProjectionTableSink` | Public API filtered projection written to a table |
| `javaPlanBuilderReadTableJoin` | Public API two-table join with pushed-down record output |

## Environment Variables

Override the default endpoint when running against another PrestoDB:

| Variable | Default |
|----------|---------|
| `PRESTO_HOST` | `localhost` |
| `PRESTO_PORT` | `8081` |
| `PRESTO_USER` | `test` |

Example:

```bash
PRESTO_HOST=my-presto PRESTO_PORT=8080 PRESTO_USER=wayang \
  ./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-presto -am \
  -Dtest=PrestoOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false \
  -DfailIfNoTests=false -Drat.skip=true -Dlicense.skip=true test
```

## Troubleshooting

### `Catalog does not exist: memory`

Check that `presto-setup/etc/catalog/memory.properties` is a regular file
before starting the container. If Docker first created the container while the
source file was absent, it may have mounted a directory at the catalog path.
Recreate the container after checking out the branch:

```bash
docker compose -f presto-setup/docker-compose.yml down
docker compose -f presto-setup/docker-compose.yml up -d --force-recreate --wait
```

Confirm the mounted path inside the container is a file:

```bash
docker exec presto sh -c \
  "ls -l /opt/presto-server/etc/catalog/memory.properties"
```

Then rerun `PrestoOperatorsIT`.
