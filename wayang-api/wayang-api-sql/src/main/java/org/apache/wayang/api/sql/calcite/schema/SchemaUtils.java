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

package org.apache.wayang.api.sql.calcite.schema;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.wayang.core.api.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * This class contains some hardcoded functions for creating calcite schema.
 * TODO: Automatically create calcite schema based on user provided configurations of table sources
 */
public class SchemaUtils {

    private static Properties getPostgresProperties() {
        /** Hardcoded for testing **/
        Properties info = new Properties();
        info.put("model",
                "inline:"
                        + "{\n"
                        + "  version: '1.0',\n"
                        + "  defaultSchema: 'imdb',\n"
                        + "  schemas: [\n"
                        + "     {\n"
                        + "       name: 'postgres',\n"
                        + "       type: 'custom',\n"
                        + "       factory: 'org.apache.wayang.api.sql.calcite.jdbc.JdbcSchema$Factory',\n"
                        + "       operand: {\n"
                        + "         jdbcDriver: 'org.postgresql.Driver',\n"
                        + "         jdbcUrl: 'jdbc:postgresql://localhost:5432/imdb',\n"
                        + "         jdbcUser: 'postgres',\n"
                        + "         jdbcPassword: 'postgres'\n"
                        + "       }\n"
                        + "     }\n"
                        + "  ]\n"
                        + "}");
        return info;
    }

    private static Properties getFileProperties() {
        Properties info = new Properties();
        info.put("model",
                "inline:"
                        + "{\n"
                        + "  version: '1.0',\n"
                        + "  defaultSchema: 'tpch',\n"
                        + "  schemas: [ {\n"
                        + "    name: 'fs',\n"
                        + "    type: 'custom',\n"
                        + "    factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
                        + "    operand: {\n"
                        + "        directory: '/data/Projects/databloom/test-data'\n"
                        + "      } \n"
                        + "    }\n"
                        + "  ]\n"
                        + "}"
        );
        return info;
    }


    public static CalciteSchema getPostgresSchema(Configuration configuration) throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:calcite:", getPostgresProperties());
        return getCalciteSchema(connection);
    }

    public static CalciteSchema getFileSchema(Configuration configuration) throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:calcite:", getFileProperties());
        return getCalciteSchema(connection);
    }

    public static CalciteSchema getCalciteSchema(Connection connection) throws SQLException {
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            final SchemaPlus schemaPlus = calciteConnection.getRootSchema();
            CalciteSchema calciteSchema = CalciteSchema.from(schemaPlus);
            return calciteSchema;
    }
}
