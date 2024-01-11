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


public class SchemaUtils {
    public static CalciteSchema getSchema(Configuration configuration) throws SQLException {
        String calciteModel = configuration.getStringProperty("wayang.calcite.model");
        final Connection connection = DriverManager.getConnection("jdbc:calcite:model=inline: "+calciteModel);
        return getCalciteSchema(connection);
    }
    public static CalciteSchema getCalciteSchema(Connection connection) throws SQLException {
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        final SchemaPlus schemaPlus = calciteConnection.getRootSchema();
        return CalciteSchema.from(schemaPlus);
    }
}
