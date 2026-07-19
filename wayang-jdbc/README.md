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

# Wayang JDBC

This module provides the external JDBC interface for Apache Wayang.

Apache Wayang is not a database and does not own table storage. The JDBC layer
is therefore designed as a read-only SQL gateway:

```text
JDBC client -> Wayang JDBC driver -> Wayang JDBC server -> Wayang SQL API -> Wayang execution
```

The first implementation target is a minimal JDBC driver that can be used by
standard Java JDBC clients and database tools for SQL query execution against
data sources configured in Wayang.

See `DEVELOPMENT_LOG.md` for the date-wise implementation log, decisions,
verification, and remaining work.

## Module Roles

`wayang-jdbc-protocol`
: Shared request and response messages plus the wire codec used by the driver
  and server.

`wayang-jdbc-driver`
: The client-side JDBC driver. It exposes the `java.sql` interfaces and talks to
  the Wayang JDBC server over the protocol module.

`wayang-jdbc-server`
: The server-side gateway. It accepts driver requests, delegates SQL execution
  to the Wayang SQL API, and returns rows and metadata to the driver.

The existing `wayang-platforms/wayang-jdbc-template`,
`wayang-platforms/wayang-postgres`, `wayang-platforms/wayang-sqlite3`, and
`wayang-platforms/wayang-generic-jdbc` modules solve the opposite direction:
Wayang reading from JDBC-accessible databases. They are not the external JDBC
client interface.

## First Supported Scope

The first usable version is intentionally read-only and limited to query
execution.

Supported first:

* `DriverManager.getConnection("jdbc:wayang://host:port[/database]")`
* `Connection.createStatement()`
* `Connection.close()`
* `Connection.isClosed()`
* `Connection.getMetaData()`
* `Statement.executeQuery(String)`
* `Statement.close()`
* `ResultSet.next()`
* `ResultSet.getObject(...)`
* `ResultSet.getString(...)`
* `ResultSet.getBoolean(...)`
* `ResultSet.getByte(...)`
* `ResultSet.getShort(...)`
* `ResultSet.getInt(...)`
* `ResultSet.getLong(...)`
* `ResultSet.getFloat(...)`
* `ResultSet.getDouble(...)`
* `ResultSet.wasNull()`
* `ResultSet.getMetaData()`
* `ResultSetMetaData` basics: column count, name, label, JDBC type, type name,
  precision, scale, nullability.
* `DatabaseMetaData` basics: schemas, tables, columns, driver/product
  information, and common capability flags.

Unsupported first:

* `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`
* transactions and savepoints
* stored procedures
* writable or scrollable result sets
* batched statements
* generated keys
* advanced SQL functions beyond the Wayang SQL API support
* full JDBC compliance

Unsupported JDBC operations should throw `SQLFeatureNotSupportedException`
unless the JDBC contract requires a default value.

## Query Execution Contract

The server delegates SQL queries to the Wayang SQL API. The SQL API currently
returns `Collection<Record>`, where `Record` contains row values only. JDBC
requires result metadata as well, so the SQL API needs an execution method that
returns both rows and the Calcite-derived row type.

The target result shape is:

```java
SqlQueryResult {
    List<ColumnInfo> columns;
    List<Record> rows;
}
```

Column metadata should be derived from the validated/optimized Calcite
`RelNode.getRowType()` before the Wayang plan is executed.

The first server implementation may keep query results in memory and expose
them through cursor IDs for fetch paging. This is acceptable for the first JDBC
gateway version. Streaming results can be added later.

## Metadata Contract

JDBC tools usually call metadata methods before query execution. The server
should answer metadata requests from the Calcite schema loaded by Wayang SQL
configuration.

Initial metadata support:

* schemas from the configured Calcite root schema
* tables from each schema table map
* columns from each table row type
* Java/Calcite type mapping to `java.sql.Types`

The driver should return metadata rows with standard JDBC column names and
ordering for the implemented `DatabaseMetaData` methods.

## Package Boundary

The current external JDBC modules use `org.apache.wayang.jdbc.driver` and
`org.apache.wayang.jdbc.protocol`. Internal JDBC platform code already uses
`org.apache.wayang.jdbc.*` under `wayang-platforms/wayang-jdbc-template`.

To keep implementation risk low, the current package layout is retained for the
first JDBC gateway implementation. Classes should still be named clearly with
driver/server/protocol responsibilities to avoid mixing external JDBC gateway
code with internal JDBC platform operators.

## Implementation Order

1. Add a SQL API result object that includes rows and metadata.
2. Implement the server dispatcher and in-memory cursor store.
3. Implement the driver socket client.
4. Implement `Connection`, `Statement`, `ResultSet`, and `ResultSetMetaData`.
5. Implement basic `DatabaseMetaData`.
6. Add end-to-end JDBC tests using `DriverManager`.
7. Run a compatibility pass with DBeaver/DataGrip and fill only the methods they
   require for browsing and query execution.
