<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Wayang JDBC Demo

This demo is the easiest way to understand how the configured CSV files map to
SQL-style table names in the Wayang JDBC demo.

Run from the repository root.

To start the Wayang JDBC server with the demo CSV directory exposed as schema
`fs`, run:

```bash
bash wayang-jdbc/demo/start-demo-server.sh
```

To inspect the available CSV files and run simple local analysis, run:

```bash
bash wayang-jdbc/demo/run-demo-client.sh
```

The server script:

* builds the JDBC server and dependencies
* creates `/tmp/wayang-jdbc-demo/wayang.properties`
* points the Calcite file schema at `wayang-jdbc/demo/data`
* starts `org.apache.wayang.jdbc.server.WayangJdbcServer` on `127.0.0.1:9999`

The analysis script:

* compiles `CsvSelectionOperationsDemo.java`
* lists CSV files from `wayang-jdbc/demo/data`
* shows each file's SQL-style table name, such as `fs.people`
* lets you select a CSV file by number or name
* prints columns, sample rows, numeric summaries, and dataset-specific analysis
  for `heart_disease_risk_2026.csv`

For example, selecting `heart_disease_risk` analyzes
`data/heart_disease_risk_2026.csv` as the logical table
`fs.heart_disease_risk_2026`.

The demo is intentionally read-only. It reads CSV files from `data/`; it does
not create, insert, update, or delete tables.
