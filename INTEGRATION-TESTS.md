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

# Integration Tests – Presto

End-to-end tests that drive the Presto platform operators (TableSource, Filter,
Projection, Join) through the Wayang API against a live PrestoDB cluster.

| Test | Module | Needs |
|------|--------|-------|
| `AllOperatorsIT` | `wayang-presto` | a local Presto (Docker) |

The test **skips** (it does not fail) when Presto is unreachable.

---

## Prerequisites

- **JDK 17** — required. The Scala in `wayang-spark` does not compile on JDK 21+,
  and the build targets release 17, so JDK 11 is too old. Point Maven at a JDK 17:
  ```bash
  export JAVA_HOME=/path/to/jdk-17      # e.g. .../corretto-17.jdk/Contents/Home
  ```
- **Maven 3.8+**
- **Docker**

### Common Maven flags

The repo's root build runs RAT + license + prerequisite checks that are noisy for
local runs; skip them:

```
-Drat.skip=true -Dlicense.skip=true -Dmaven.javadoc.skip=true -Pskip-prerequisite-check
```

> First build only: drop `-o` (offline) so Maven can download dependencies.

---

## Presto

The test is self-contained: it creates and seeds its own `memory.wayang_it`
tables in Presto's built-in **in-memory connector** (scaled to 120k rows so the
optimizer elects SQL pushdown) and drops them afterwards — no Hive metastore or
object storage required.

```bash
# 1. start a single PrestoDB node with the in-memory connector
cd presto-setup && docker compose up -d --wait && cd ..

# 2. run the operator tests (JDK 17)
JAVA_HOME=/path/to/jdk-17 \
mvn -o test -pl wayang-platforms/wayang-presto \
  -Dtest=AllOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false \
  -Drat.skip=true -Dlicense.skip=true -Dmaven.javadoc.skip=true -Pskip-prerequisite-check

# 3. tear down when done
cd presto-setup && docker compose down -v && cd ..
```

Expected: `Tests run: 4, Failures: 0, Errors: 0, Skipped: 0`.

`docker compose up -d --wait` blocks on the container healthcheck, so Presto is
query-ready when it returns. Presto listens on host port **8081** (container 8080).

---

## Notes

- **Pushdown is cost-gated.** On tiny tables Wayang's optimizer prefers a full
  scan + Java-side filter/projection, so pushdown only appears once a table is
  large enough (hence the test scales to 120k rows). Each test asserts both correct
  results and that the expected SQL reached Presto (`system.runtime.queries`).
- **Join.** A JDBC join is verified through the operator's SQL-clause contract
  executed on Presto, not the high-level `WayangContext` API — the logical
  `JoinOperator` emits `Tuple2<Record,Record>`, which cannot connect to a `Record`
  sink before the SQL pushdown flattens it.
- **Trailing semicolons.** Presto's SQL parser rejects a trailing `;` in
  `executeQuery`, so this branch also carries the jdbc-template change that stops
  emitting one (shared with the other JDBC platforms; Postgres/SQLite tolerate its
  absence).
