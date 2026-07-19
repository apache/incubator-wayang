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

# Wayang JDBC Implementation Plan

## Goal

Build the first external JDBC interface for Apache Wayang.

Target architecture:

```text
JDBC client -> Wayang JDBC driver -> Wayang JDBC server -> Wayang SQL API -> Wayang execution
```

Wayang is not a database and does not own table storage. The JDBC layer is a
read-only SQL gateway for query execution and metadata browsing.

Unsupported write or database-owned operations should throw
`SQLFeatureNotSupportedException` unless JDBC requires a default value.

## Scope

Supported first:

* Open JDBC connections through `DriverManager`.
* Create JDBC statements.
* Execute read-only SQL queries.
* Read result rows through `ResultSet`.
* Read result column metadata through `ResultSetMetaData`.
* Browse basic metadata later through `DatabaseMetaData`.

Unsupported first:

* `CREATE`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`.
* Transactions and savepoints.
* Stored procedures.
* Writable result sets.
* Batch updates.
* Generated keys.
* Full JDBC compliance.

## Start To End Plan

1. Create JDBC module structure.
   * Add `wayang-jdbc-protocol`.
   * Add `wayang-jdbc-driver`.
   * Add `wayang-jdbc-server`.

2. Define shared protocol.
   * Add message envelope.
   * Add message types.
   * Add request and response payloads.
   * Add length-prefixed JSON codec.
   * Add protocol tests.

3. Add driver registration and URL parsing.
   * Add `WayangDriver`.
   * Add `WayangJdbcUrl`.
   * Add service loader file for `java.sql.Driver`.
   * Support URL format `jdbc:wayang://host:port[/database]`.
   * Add URL parsing tests.

4. Extend Wayang SQL API for metadata.
   * Add SQL result object with rows and columns.
   * Add SQL column metadata model.
   * Add `SqlContext.executeSqlWithMetadata(String)`.
   * Keep old `executeSql(String)` behavior.
   * Derive JDBC metadata from Calcite row types.

5. Implement server-side protocol bridge.
   * Add TCP server.
   * Add client handler.
   * Add request dispatcher.
   * Add logical session manager.
   * Add in-memory cursor store.
   * Add SQL query executor boundary.
   * Adapt SQL API results into protocol rows and columns.
   * Add dispatcher and socket tests.

6. Implement driver-side protocol client.
   * Open socket to server.
   * Send `OPEN_CONNECTION`.
   * Store server `connectionId`.
   * Send `EXECUTE_QUERY`.
   * Send `FETCH`.
   * Send `CLOSE_CURSOR`.
   * Send `CANCEL_QUERY`.
   * Send `CLOSE_CONNECTION`.
   * Convert protocol errors into `SQLException`.
   * Add driver-only socket tests.

7. Implement JDBC `Connection`.
   * Add `WayangConnection`.
   * Hold `WayangJdbcClient`.
   * Implement `close`.
   * Implement `isClosed`.
   * Implement read-only and auto-commit defaults.
   * Keep transactions unsupported.
   * Wire `WayangDriver.connect(...)` to return `WayangConnection`.
   * Add connection tests.

8. Implement JDBC `Statement`.
   * Add `WayangStatement`.
   * Implement `executeQuery(String)`.
   * Generate statement IDs.
   * Support fetch size.
   * Close current result set on statement close.
   * Reject update and batch methods.
   * Add statement tests.

9. Implement JDBC `ResultSet`.
   * Add `WayangResultSet`.
   * Hold first query result batch.
   * Implement `next`.
   * Fetch more rows when cursor has more data.
   * Implement `getObject`.
   * Implement `getString`.
   * Implement primitive getters.
   * Implement `wasNull`.
   * Close server cursor when needed.
   * Add result set tests.

10. Implement JDBC `ResultSetMetaData`.
    * Add `WayangResultSetMetaData`.
    * Use protocol `ColumnInfo`.
    * Implement column count.
    * Implement column name and label.
    * Implement JDBC type and type name.
    * Implement precision, scale, and nullability.
    * Add metadata tests.

11. Add end-to-end JDBC tests.
    * Start `WayangJdbcServer` with fake SQL executor.
    * Connect through `DriverManager`.
    * Create statement.
    * Execute query.
    * Read rows.
    * Read result metadata.
    * Verify cursor paging.
    * Verify close behavior.

12. Implement basic `DatabaseMetaData`.
    * Add driver and product information.
    * Add read-only capability flags.
    * Add schemas, tables, and columns after server metadata support exists.
    * Keep unsupported metadata explicit.

13. Test with JDBC tools.
    * Try DBeaver.
    * Try DataGrip.
    * Implement only required additional JDBC methods.
    * Keep unsupported write operations explicit.

## Completed Work In Sequence

1. Added module structure.
   * `wayang-jdbc/wayang-jdbc-protocol`
   * `wayang-jdbc/wayang-jdbc-driver`
   * `wayang-jdbc/wayang-jdbc-server`

2. Added documentation.
   * `wayang-jdbc/README.md`
   * `wayang-jdbc/DEVELOPMENT_LOG.md`

3. Added protocol module.
   * `MessageEnvelope`
   * `MessageType`
   * `ProtocolConstants`
   * `ProtocolException`
   * `ProtocolMessageCodec`
   * Connection messages.
   * Query messages.
   * Fetch messages.
   * Cursor close messages.
   * Cancel messages.
   * Metadata message placeholders.
   * Error messages.

4. Added initial driver skeleton.
   * `WayangDriver`
   * `WayangJdbcUrl`
   * `META-INF/services/java.sql.Driver`
   * URL parsing tests.

5. Added SQL API query metadata support.
   * `SqlColumn`
   * `SqlQueryResult`
   * `SqlContext.executeSqlWithMetadata(String)`
   * Existing `executeSql(String)` still works.

6. Added server-side JDBC bridge.
   * `WayangJdbcServer`
   * `JdbcClientHandler`
   * `JdbcRequestDispatcher`
   * `JdbcServerSessionManager`
   * `CursorStore`
   * `SqlQueryExecutor`
   * `WayangSqlQueryExecutor`
   * `SqlQueryResultAdapter`

7. Server can now:
   * Open logical connections.
   * Execute SQL through `SqlContext.executeSqlWithMetadata`.
   * Return rows.
   * Return column metadata.
   * Page results through in-memory cursors.
   * Fetch more rows.
   * Close cursors.
   * Close connections.
   * Return protocol errors.

8. Added driver-side protocol client.
   * `WayangJdbcClient`
   * Opens socket to server.
   * Sends `OPEN_CONNECTION`.
   * Sends `EXECUTE_QUERY`.
   * Sends `FETCH`.
   * Sends `CLOSE_CURSOR`.
   * Sends `CANCEL_QUERY`.
   * Sends `CLOSE_CONNECTION`.
   * Converts protocol errors into JDBC exceptions.

9. Added JDBC connection layer.
   * `WayangConnection`
   * `WayangDriver.connect(...)` now opens `WayangJdbcClient`.
   * `WayangDriver.connect(...)` now returns a read-only JDBC connection.
   * Connection close sends `CLOSE_CONNECTION`.
   * Transactions, savepoints, metadata, statements, prepared statements, and
     callable statements are still explicitly unsupported.

10. Test classes are deferred for the final implementation pass.
   * Re-add focused unit tests after the main JDBC path is complete.
   * Add end-to-end JDBC tests after `Connection`, `Statement`, `ResultSet`,
     and `ResultSetMetaData` work together.

11. Added JDBC statement and query result layer.
   * `WayangStatement`
   * `WayangConnection.createStatement()` now returns a statement.
   * `Statement.executeQuery(String)` sends queries through `WayangJdbcClient`.
   * `WayangResultSet`
   * `WayangResultSetMetaData`
   * Result sets can move through rows with `next`.
   * Result sets can fetch more rows from server cursors.
   * Result sets support basic object, string, primitive, and metadata access.
   * Result set close releases server cursors when needed.
   * Update, batch, generated-key, writable result set, and unsupported methods
     remain explicit unsupported operations.

12. Hardened plain Java JDBC result handling.
   * `Statement.setMaxRows(...)` is applied during result iteration.
   * Reaching the maximum row limit closes the server cursor when needed.
   * Added common result getters for character streams, binary streams,
     `getNString`, and URL values.
   * Added explicit unsupported handling for SQL ARRAY, BLOB, CLOB, NCLOB, and
     SQLXML result values.
   * Added read-only row state defaults for `rowUpdated`, `rowInserted`, and
     `rowDeleted`.
   * Kept scroll navigation unsupported because result sets are forward-only.

13. Added basic JDBC database metadata.
   * `Connection.getMetaData()` now returns metadata instead of unsupported.
   * Metadata reports Apache Wayang driver and product information.
   * Metadata reports read-only capability flags.
   * Transactions, stored procedures, batches, generated keys, savepoints, and
     writable result sets are reported unsupported.
   * Schemas, catalogs, tables, columns, table types, type info, keys, indexes,
     procedures, functions, and other common metadata methods return JDBC
     `ResultSet` objects.
   * Table and column browsing returns empty result sets until server-side
     metadata discovery is connected.

14. Connected metadata requests through the JDBC protocol.
   * Driver sends `GET_SCHEMAS`, `GET_TABLES`, and `GET_COLUMNS`.
   * Server dispatches metadata requests and returns `METADATA_RESULT`.
   * Server tracks connection database information in logical sessions.
   * Added a server-side `SqlMetadataProvider` boundary.
   * Default provider returns schema metadata from the connection database.
   * Default table and column metadata still return correctly shaped empty
     result sets until Calcite/catalog-backed discovery is added.

## Current State

Working:

* Protocol layer.
* URL parsing.
* SQL API metadata result support.
* JDBC server query bridge.
* Driver-side protocol client.
* `DriverManager.getConnection(...)` path up to `WayangConnection`.
* Read-only JDBC connection defaults and close behavior.
* `Connection.createStatement()`.
* `Statement.executeQuery(String)`.
* Forward-only read-only result sets.
* Basic `ResultSetMetaData`.
* Basic `DatabaseMetaData`.
* Metadata protocol path for schemas, tables, and columns.
* Plain Java JDBC query flow for connection, statement, result set iteration,
  getters, metadata, and close behavior.

Not complete yet:

* Result set support still needs final compatibility testing.
* JDBC `DatabaseMetaData` table and column discovery still needs Calcite or
  catalog-backed metadata rows.

## Next Immediate Work

1. Review and harden result set method coverage.
2. Back `SqlMetadataProvider` with Calcite or configured catalog metadata.
3. Add end-to-end `DriverManager` query tests in the final test pass.
4. Add focused tests for cursor paging, metadata, getters, and close behavior.

## Verification Commands Used

```bash
env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH ./mvnw -pl :wayang-jdbc-protocol,:wayang-jdbc-driver,:wayang-jdbc-server -am test
```

```bash
env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH ./mvnw -pl :wayang-api-sql -am test
```

```bash
env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH ./mvnw -pl :wayang-jdbc-server -am test
```

```bash
env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH ./mvnw -pl :wayang-jdbc-driver -am test
```
