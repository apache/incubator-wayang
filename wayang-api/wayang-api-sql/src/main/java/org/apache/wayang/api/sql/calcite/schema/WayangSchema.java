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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

public class WayangSchema extends AbstractSchema {

    private final String schemaName;
    private final Map<String, Table> tableMap;


    public WayangSchema(String schemaName, Map<String, Table> tableMap) {
        this.schemaName = schemaName;
        this.tableMap = tableMap;
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMap;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        return this;
    }
}
