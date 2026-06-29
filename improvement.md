<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# BigQuery engine-only integration test

## 1. What this branch demonstrates

The question this branch answers is **not** "does BigQuery execute some single
operator?" but:

> From `WayangContext.execute(...)` to the end of the whole Wayang plan, do all
> data processing **and** the final sink run inside BigQuery, **without**
> registering `Java.basicPlugin()`?

On this branch the answer is **yes**. `BigQueryOperatorsIT`:

- registers **only** `BigQuery.plugin()` — no `Java.basicPlugin()`;
- ends **every** Wayang plan in a BigQuery `TableSink`, which compiles to a single
  `CREATE TABLE \`project.dataset.table\` AS SELECT ...` executed inside BigQuery;
- after `WayangContext.execute(...)` returns, JUnit reads the result table with a
  plain JDBC query (assertion only — not part of the Wayang plan);
- handles the join `Tuple2<Record, Record>` vs flat `Record` mismatch with a
  test-only flatten mapping (see §4). This is a test-only scheme, not a final
  decision on Tuple-to-Record semantics for JDBC platforms.

This mirrors the Trino-only work on `wayang-trino-only-test`; the contrast is the
older mixed branch `wayang-bigquery`, which registered both `Java.basicPlugin()`
and `BigQuery.plugin()` and ended most operator tests in a Java `LocalCallbackSink`
or `.collect()`.

## 2. Execution shape

```text
BigQuery TableSource -> BigQuery operator(s) -> BigQuery TableSink
                                                  |
                                                  v
                          CREATE TABLE `proj.sales.wayang_operator_result` AS SELECT ...

WayangContext.execute(...) returns
                                                  |
                                                  v
                             JUnit queries the result table over JDBC (assertions only)
```

The final JDBC query is part of the test only: it is not in the Wayang logical
plan, it is not a Wayang Java execution operator, and it does not process plan
data on BigQuery's behalf — it just inspects what BigQuery already wrote.

Because **no** `Java.basicPlugin()` is registered, the optimizer has no Java
operators to fall back to, so pushdown is forced — the small reference table does
not need to be scaled to make pushdown the cheaper plan, and the sink table
appearing in BigQuery with the correct contents is itself proof that the
`CREATE TABLE ... AS SELECT` ran inside BigQuery.

## 3. The shared executor change

All JDBC platforms share `wayang-jdbc-template`'s `JdbcExecutor`. When a stage's
terminal task is a `JdbcTableSinkOperator`, `JdbcExecutor.executeSinkStage(...)`
composes and runs the `CREATE TABLE ... AS SELECT` directly on the connection.

The previous BigQuery branch's `executeSinkStage` (identical to `wayang-trino`'s)
had two gaps that only surface once **every** test ends in a `TableSink`:

1. It used `selectStartTask(...)` only on the normal query-channel path, not in the
   sink path, where it asserted a single source — so a join (two sources) could not
   be composed into the sink.
2. It only collected filter, projection and join; it threw `WayangException` for
   global reduce, reduce-by and sort, and passed `null` for them to
   `createSqlString(...)`.

This branch ports the engine-only `executeSinkStage` (identical to the file on
`wayang-trino-only-test`): it uses `selectStartTask(...)` for multi-source joins
and collects global reduce / reduce-by / sort, passing them into the existing
`createSqlString(...)`. The file is platform-agnostic. (Assertions are enabled
under Maven — `pom.xml` `enableAssertions=true` — so without this change a
join/reduce/sort sink would fail loudly, not silently.)

BigQuery dialect notes: the generated SQL is dialect-valid — backtick-quoted
fully-qualified table names, no trailing semicolon, and `CREATE TABLE ... AS` /
`DROP TABLE IF EXISTS` (DDL) only, never DML — so the suite runs on a free-tier
(no-billing) project.

## 4. The join flatten mapping

A logical `JoinOperator` emits `Tuple2<Record, Record>`, while a pushed-down JDBC
join already emits a flat `Record`. The test wires an explicit flatten `MapOperator`
(named `JOIN_FLATTEN_NAME`) and registers a test-only `JoinFlattenMapping` on the
configuration whitelist; the mapping rewrites that named map into a
`BigQueryProjectionOperator`, so the flatten is also pushed into BigQuery SQL and
the plan stays entirely in BigQuery. The join lookup table's key column is renamed
to `region_name` so the flattened `CREATE TABLE AS SELECT` has no duplicate column.

## 5. Coverage and results

`BigQueryOperatorsIT` runs 13 tests (8 operator-level + 5 high-level
`JavaPlanBuilder`) covering `TableSource`, `Filter`, `Projection`, `Join`,
`GlobalReduce`, `ReduceBy`, `Sort`, `TableSink`. Each composes a
`CREATE TABLE ... AS SELECT` executed inside BigQuery.

Unlike Trino/Presto, this suite runs against **real BigQuery** (the JDBC driver
needs OAuth2; the local emulator cannot serve it), so it requires a live GCP
project + service account. If a connection cannot be established the whole class
is skipped (not failed).

```bash
JAVA_HOME=<jdk17> mvn test -pl wayang-platforms/wayang-bigquery -am \
  -Dtest=BigQueryOperatorsIT -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false \
  -Dbigquery.project=YOUR_PROJECT_ID \
  -Dbigquery.saEmail=wayang-bq@YOUR_PROJECT_ID.iam.gserviceaccount.com \
  -Dbigquery.keyPath=$HOME/wayang-bq-key.json \
  -Drat.skip=true -Dlicense.skip=true -Pskip-prerequisite-check
```

The reference table (default `<project>.sales.orders`, 10 rows) must be seeded
first (see the setup notes); the suite creates and drops its own
`sales.wayang_operator_result` and `sales.wayang_regions` tables. Expected:
`Tests run: 13, Failures: 0, Errors: 0, Skipped: 0`.
