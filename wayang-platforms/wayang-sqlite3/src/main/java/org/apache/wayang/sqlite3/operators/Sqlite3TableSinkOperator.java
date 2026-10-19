/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.sqlite3.operators;

import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.jdbc.operators.JdbcTableSinkOperator;
import org.apache.wayang.sqlite3.platform.Sqlite3Platform;

/**
 * SQLite3 implementation of the {@link JdbcTableSinkOperator}.
 */
public class Sqlite3TableSinkOperator extends JdbcTableSinkOperator {

    public Sqlite3TableSinkOperator(String tableName, String[] columnNames) {
        super(tableName, columnNames);
    }

    public Sqlite3TableSinkOperator(TableSink<Record> that) {
        super(that);
    }

    @Override
    public Sqlite3Platform getPlatform() {
        return Sqlite3Platform.getInstance();
    }
}