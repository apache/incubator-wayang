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

import org.apache.wayang.jdbc.protocol.message.ColumnInfo;
import org.apache.wayang.jdbc.protocol.message.GetColumnsRequest;
import org.apache.wayang.jdbc.protocol.message.GetSchemasRequest;
import org.apache.wayang.jdbc.protocol.message.GetTablesRequest;
import org.apache.wayang.jdbc.protocol.message.MetadataResultResponse;
import org.apache.wayang.jdbc.protocol.message.MetadataType;

import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Default metadata provider for the read-only Wayang JDBC gateway.
 */
class DefaultSqlMetadataProvider implements SqlMetadataProvider {

    @Override
    public MetadataResultResponse getSchemas(
            final JdbcServerSession session,
            final GetSchemasRequest request
    ) {
        final List<List<Object>> rows = new ArrayList<>();
        final String schema = session.getDatabase();
        final String catalog = session.getDatabase();
        if (schema != null
                && this.matchesCatalog(catalog, request.getCatalog())
                && this.matchesPattern(schema, request.getSchemaPattern())) {
            rows.add(Arrays.asList(schema, catalog));
        }

        return new MetadataResultResponse(
                session.getConnectionId(),
                MetadataType.SCHEMAS,
                this.schemasColumns(),
                rows
        );
    }

    @Override
    public MetadataResultResponse getTables(
            final JdbcServerSession session,
            final GetTablesRequest request
    ) {
        return new MetadataResultResponse(
                session.getConnectionId(),
                MetadataType.TABLES,
                this.tablesColumns(),
                Collections.emptyList()
        );
    }

    @Override
    public MetadataResultResponse getColumns(
            final JdbcServerSession session,
            final GetColumnsRequest request
    ) {
        return new MetadataResultResponse(
                session.getConnectionId(),
                MetadataType.COLUMNS,
                this.columnsColumns(),
                Collections.emptyList()
        );
    }

    private List<ColumnInfo> schemasColumns() {
        return Arrays.asList(
                column("TABLE_SCHEM", Types.VARCHAR),
                column("TABLE_CATALOG", Types.VARCHAR)
        );
    }

    private List<ColumnInfo> tablesColumns() {
        return Arrays.asList(
                column("TABLE_CAT", Types.VARCHAR),
                column("TABLE_SCHEM", Types.VARCHAR),
                column("TABLE_NAME", Types.VARCHAR),
                column("TABLE_TYPE", Types.VARCHAR),
                column("REMARKS", Types.VARCHAR),
                column("TYPE_CAT", Types.VARCHAR),
                column("TYPE_SCHEM", Types.VARCHAR),
                column("TYPE_NAME", Types.VARCHAR),
                column("SELF_REFERENCING_COL_NAME", Types.VARCHAR),
                column("REF_GENERATION", Types.VARCHAR)
        );
    }

    private List<ColumnInfo> columnsColumns() {
        return Arrays.asList(
                column("TABLE_CAT", Types.VARCHAR),
                column("TABLE_SCHEM", Types.VARCHAR),
                column("TABLE_NAME", Types.VARCHAR),
                column("COLUMN_NAME", Types.VARCHAR),
                column("DATA_TYPE", Types.INTEGER),
                column("TYPE_NAME", Types.VARCHAR),
                column("COLUMN_SIZE", Types.INTEGER),
                column("BUFFER_LENGTH", Types.INTEGER),
                column("DECIMAL_DIGITS", Types.INTEGER),
                column("NUM_PREC_RADIX", Types.INTEGER),
                column("NULLABLE", Types.INTEGER),
                column("REMARKS", Types.VARCHAR),
                column("COLUMN_DEF", Types.VARCHAR),
                column("SQL_DATA_TYPE", Types.INTEGER),
                column("SQL_DATETIME_SUB", Types.INTEGER),
                column("CHAR_OCTET_LENGTH", Types.INTEGER),
                column("ORDINAL_POSITION", Types.INTEGER),
                column("IS_NULLABLE", Types.VARCHAR),
                column("SCOPE_CATALOG", Types.VARCHAR),
                column("SCOPE_SCHEMA", Types.VARCHAR),
                column("SCOPE_TABLE", Types.VARCHAR),
                column("SOURCE_DATA_TYPE", Types.SMALLINT),
                column("IS_AUTOINCREMENT", Types.VARCHAR),
                column("IS_GENERATEDCOLUMN", Types.VARCHAR)
        );
    }

    private boolean matchesCatalog(final String value, final String catalog) {
        if (catalog == null) {
            return true;
        }
        return catalog.equals(value);
    }

    private boolean matchesPattern(final String value, final String pattern) {
        if (pattern == null) {
            return true;
        }
        if (value == null) {
            return false;
        }
        return Pattern.compile(this.toRegex(pattern), Pattern.CASE_INSENSITIVE).matcher(value).matches();
    }

    private String toRegex(final String pattern) {
        final StringBuilder regex = new StringBuilder();
        for (int index = 0; index < pattern.length(); index++) {
            final char current = pattern.charAt(index);
            if (current == '%') {
                regex.append(".*");
            } else if (current == '_') {
                regex.append('.');
            } else {
                regex.append(Pattern.quote(String.valueOf(current)));
            }
        }
        return regex.toString();
    }

    private static ColumnInfo column(final String name, final int jdbcType) {
        return new ColumnInfo(
                name,
                name,
                "",
                "",
                jdbcTypeName(jdbcType),
                jdbcType,
                ResultSetMetaData.columnNullable,
                0,
                0
        );
    }

    private static String jdbcTypeName(final int jdbcType) {
        switch (jdbcType) {
            case Types.INTEGER:
                return "INTEGER";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.VARCHAR:
                return "VARCHAR";
            default:
                return "OTHER";
        }
    }
}
