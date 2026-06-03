# Trino Local Setup

Local Trino environment backed by an **Iceberg** data lake, completely containerised.

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
├── docker-compose.yml          # Full stack definition
├── trino/
│   ├── config.properties       # Trino node config
│   └── catalog/
│       ├── iceberg.properties  # Iceberg via HMS + MinIO
│       └── tpch.properties     # Built-in TPC-H (no storage needed)
├── scripts/
│   ├── init.sql                # Creates iceberg.sales.orders + sample rows
│   └── run-init.sh             # Helper: waits for Trino then runs init.sql
├── pom.xml                     # Standalone Maven project (Java 17)
└── src/test/java/.../
    └── TrinoIntegrationTest.java   # JUnit 5 integration tests
```

## Quick Start

### 1. Start the stack

```bash
cd trino-setup
docker-compose up -d
```

Wait ~30 seconds for all services to become healthy. Check with:

```bash
docker-compose ps
# or watch the Trino UI at http://localhost:8080
```

### 2. Load sample Iceberg data

```bash
./scripts/run-init.sh
```

This creates the schema `iceberg.sales` and inserts 10 sample orders into
`iceberg.sales.orders` (Parquet files on MinIO).

### 3. Run the integration tests

```bash
mvn test -Pintegration
```

Tests are skipped by default (no `-Pintegration`) to avoid requiring Docker in CI.

### 4. Manual exploration

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

### 5. Tear down

```bash
docker-compose down -v   # -v removes volumes (clears MinIO data)
```

## Test Coverage

| Test | What it checks |
|------|----------------|
| `testConnectivity` | `SELECT 1` — JDBC connection works |
| `testTpchConnector` | TPC-H built-in connector, no storage needed |
| `testTpchTopOrders` | ORDER BY + LIMIT on TPC-H |
| `testIcebergSchemaVisible` | Schema created by `init.sql` is visible |
| `testIcebergSelectAll` | Full table scan, 10 rows |
| `testIcebergFilterByRegion` | WHERE pushdown on string column |
| `testIcebergAggregate` | GROUP BY + SUM aggregation |
| `testIcebergFilterByAmount` | WHERE pushdown on double column |
| `testIcebergProjection` | SELECT subset of columns |
| `testIcebergFilesMetadata` | `$files` system table, confirms Parquet on MinIO |

## Environment Variables

Override defaults if running Trino on a different host/port:

```bash
TRINO_HOST=my-trino-host TRINO_PORT=8080 mvn test -Pintegration
```
