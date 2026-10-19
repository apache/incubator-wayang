---
license: |
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at
         http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
layout: default
title: "API JDBC"
previous:
    url: /using_wayang/api_sql/
    title: API SQL
---

# Apache Wayang JDBC Driver

> Note: The JDBC driver is under active development and may not yet support all SQL features.

The Wayang JDBC driver allows SQL clients and BI tools to connect to Apache Wayang
using the standard JDBC interface. SQL queries are delegated to Wayang's SQL API (`api-sql`)
and executed across Wayang's supported platforms (Java Streams, Apache Spark, etc.).

---

## Getting Started

To use the JDBC driver:

1. Build Apache Wayang including the `wayang-api-jdbc` module
2. Add the generated JDBC JAR to your classpath
3. Prepare a Wayang configuration file pointing to your data source

---

## Connection

Connect using the following JDBC URL format:
```
jdbc:wayang:/path/to/wayang-config.properties
```

### Example (Java)
```java
// Driver is auto-registered via Java SPI - Class.forName is optional
Class.forName("org.apache.wayang.api.jdbc.WayangDriver");

Connection conn = DriverManager.getConnection(
    "jdbc:wayang:/path/to/wayang-config.properties"
);

Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery("SELECT * FROM myTable");
while (rs.next()) {
    System.out.println(rs.getString("name") + " " + rs.getInt("value"));
}

rs.close();
stmt.close();
conn.close();
```

### PreparedStatement Example
```java
PreparedStatement ps = conn.prepareStatement(
    "SELECT * FROM myTable WHERE value > ?"
);
ps.setInt(1, 100);
ResultSet rs = ps.executeQuery();
```

---

## Configuration

The JDBC URL points to a Wayang configuration properties file.

Example `wayang-config.properties`:
```properties
wayang.calcite.model={}
wayang.fs.table.url=file:///path/to/data.csv
wayang.ml.experience.enabled=false
wayang.ml.executions.file=mle.txt
wayang.ml.optimizations.file=mlo.txt
```

---

## Supported SQL

SQL execution is handled by Wayang's `api-sql` module. The following SQL operations are generally supported:

| Operation   | Example                                                                                   |
|-------------|-------------------------------------------------------------------------------------------|
| SELECT      | `SELECT * FROM customers`                                                                 |
| Projection  | `SELECT name, age FROM customers`                                                         |
| Filter      | `SELECT * FROM customers WHERE age > 30`                                                  |
| JOIN        | `SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id`          |
| Aggregation | `SELECT COUNT(*) FROM customers`                                                          |
| GROUP BY    | `SELECT age, COUNT(*) FROM customers GROUP BY age`                                        |
| ORDER BY    | `SELECT * FROM customers ORDER BY age DESC`                                               |

---

## Metadata Support

The driver implements standard JDBC metadata interfaces used by BI tools and SQL clients:

**DatabaseMetaData:**
- `getTables()` - lists available tables
- `getColumns()` - lists column metadata
- `getSchemas()` - lists available schemas
- `getDatabaseProductName()` - returns "Apache Wayang"
- `getDriverName()` - returns "Wayang JDBC Driver"
- `getJDBCMajorVersion()` / `getJDBCMinorVersion()` - returns 4.2

**ResultSetMetaData:**
- `getColumnCount()` - number of columns in result
- `getColumnName(i)` - column name by index
- `getColumnType(i)` - column SQL type by index

---

## BI Tool Integration

The driver registers automatically via Java SPI (`META-INF/services/java.sql.Driver`),
making it discoverable by JDBC-compatible tools without manual registration.

**DBeaver setup:**

1. Go to **Database > Driver Manager > New**
2. Set Driver Class: `org.apache.wayang.api.jdbc.WayangDriver`
3. Set URL Template: `jdbc:wayang:{config_path}`
4. Add the Wayang JDBC JAR to the driver libraries
5. Connect and browse schemas via the schema browser

---

## How It Works

The driver delegates all SQL execution to Wayang's existing `api-sql` module:
```
SQL Client / BI Tool
        |  JDBC
        v
WayangDriver
        |
        v
WayangConnection
        |
        v
WayangStatement / WayangPreparedStatement
        |
        v
SqlContext.executeSql(sql)
        |
        v
Wayang Optimizer (Apache Calcite)
        |
        v
WayangPlan execution
        |
        v
Java Streams / Apache Spark / Apache Flink / ...
```

The `WayangDriver` parses the JDBC URL to locate the configuration file,
creates a `SqlContext`, and passes SQL strings directly to `SqlContext.executeSql()`.
Results are returned as a `Collection<Record>` and wrapped in a `WayangResultSet`.

---

## Limitations

- Write operations (`INSERT`, `UPDATE`, `DELETE`) are not supported
- Transactions are not supported
- `DatabaseMetaData` coverage is partial; advanced schema introspection may not be available
- Complex nested subqueries may not be supported depending on the execution platform
- Performance depends on the configured Wayang execution platform

---

## Related

- [API SQL](../api_sql/) - the SQL API that the JDBC driver delegates to
- [WAYANG-55](https://issues.apache.org/jira/browse/WAYANG-55) - JIRA issue tracking this feature

