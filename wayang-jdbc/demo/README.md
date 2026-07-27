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

This demo is the easiest way to verify the external Wayang JDBC interface with
a plain Java client.

Run from the repository root.

Terminal 1:

```bash
bash wayang-jdbc/demo/start-demo-server.sh
```

Terminal 2:

```bash
bash wayang-jdbc/demo/run-demo-client.sh
```

The server script:

* builds the JDBC server and dependencies
* creates `/tmp/wayang-jdbc-demo/wayang.properties`
* points the Calcite file schema at `wayang-jdbc/demo/data`
* starts `org.apache.wayang.jdbc.server.WayangJdbcServer` on `127.0.0.1:9999`

The client script:

* builds the JDBC driver and dependencies
* compiles `WayangJdbcDemoClient.java`
* connects to `jdbc:wayang://127.0.0.1:9999/demo`
* runs:

```sql
SELECT ID, NAME, CITY FROM fs.people ORDER BY ID
```

The demo is intentionally read-only. It reads `data/people.csv`; it does not
create, insert, update, or delete tables.
