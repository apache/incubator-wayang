/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

public class WayangJdbcDemoClient {

    public static void main(final String[] args) throws Exception {
        Class.forName("org.apache.wayang.jdbc.driver.WayangDriver");

        final Properties properties = new Properties();
        properties.setProperty("user", "demo");
        properties.setProperty("connectTimeout", "5000");

        final String url = "jdbc:wayang://127.0.0.1:9999/demo";
        final String sql = "SELECT ID, NAME, CITY FROM fs.people ORDER BY ID";

        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {

            System.out.println("Connected to " + url);
            System.out.println("Query: " + sql);
            System.out.println();

            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                for (int column = 1; column <= columnCount; column++) {
                    if (column > 1) {
                        System.out.print(" | ");
                    }
                    System.out.print(metaData.getColumnLabel(column) + "=" + resultSet.getObject(column));
                }
                System.out.println();
            }
        }
    }
}
