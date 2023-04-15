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

import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

public class WayangTableBuilder {

    private final String tableName;
    private final List<String> fieldNames = new ArrayList<>();
    private final List<SqlTypeName> fieldTypes = new ArrayList<>();
    private long rowCount;

    private WayangTableBuilder(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }

        this.tableName = tableName;
    }

    public static WayangTableBuilder build(String tableName) {
        return new WayangTableBuilder(tableName);
    }

    public WayangTableBuilder addField(String name, SqlTypeName typeName) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }

        if (fieldNames.contains(name)) {
            throw new IllegalArgumentException("Field already defined: " + name);
        }

        fieldNames.add(name);
        fieldTypes.add(typeName);

        return this;
    }

    public WayangTableBuilder withRowCount(long rowCount) {
        this.rowCount = rowCount;

        return this;
    }

    public WayangTable build() {
        if (fieldNames.isEmpty()) {
            throw new IllegalStateException("Table must have at least one field");
        }

        if (rowCount == 0L) {
            throw new IllegalStateException("Table must have positive row count");
        }

        return new WayangTable(tableName, fieldNames, fieldTypes, new WayangTableStatistic(rowCount));
    }
}
