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

package org.apache.wayang.jdbc.server;

import org.apache.wayang.api.sql.context.SqlColumn;
import org.apache.wayang.api.sql.context.SqlQueryResult;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.jdbc.protocol.message.ColumnInfo;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts SQL API results into JDBC protocol result structures.
 */
final class SqlQueryResultAdapter {

    private SqlQueryResultAdapter() {
    }

    static List<ColumnInfo> toColumnInfo(final SqlQueryResult result) {
        return result.getColumns().stream()
                .map(SqlQueryResultAdapter::toColumnInfo)
                .collect(Collectors.toList());
    }

    static List<List<Object>> toRows(final SqlQueryResult result) {
        final List<List<Object>> rows = new ArrayList<>();
        for (final Record record : result.getRows()) {
            rows.add(new ArrayList<>(Arrays.asList(record.getValues())));
        }
        return rows;
    }

    private static ColumnInfo toColumnInfo(final SqlColumn column) {
        return new ColumnInfo(
                column.getName(),
                column.getLabel(),
                null,
                null,
                column.getTypeName(),
                column.getJdbcType(),
                column.isNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls,
                column.getPrecision(),
                column.getScale()
        );
    }
}
