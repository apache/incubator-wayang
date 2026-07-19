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

package org.apache.wayang.jdbc.driver;

import org.apache.wayang.jdbc.protocol.message.ColumnInfo;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

class WayangResultSetMetaData implements ResultSetMetaData {

    private final List<ColumnInfo> columns;

    WayangResultSetMetaData(final List<ColumnInfo> columns) {
        this.columns = List.copyOf(columns);
    }

    @Override
    public int getColumnCount() {
        return this.columns.size();
    }

    @Override
    public boolean isAutoIncrement(final int column) throws SQLException {
        this.column(column);
        return false;
    }

    @Override
    public boolean isCaseSensitive(final int column) throws SQLException {
        this.column(column);
        return true;
    }

    @Override
    public boolean isSearchable(final int column) throws SQLException {
        this.column(column);
        return true;
    }

    @Override
    public boolean isCurrency(final int column) throws SQLException {
        this.column(column);
        return false;
    }

    @Override
    public int isNullable(final int column) throws SQLException {
        return this.column(column).getNullable();
    }

    @Override
    public boolean isSigned(final int column) throws SQLException {
        switch (this.column(column).getJdbcType()) {
            case Types.BIGINT:
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.INTEGER:
            case Types.NUMERIC:
            case Types.REAL:
            case Types.SMALLINT:
            case Types.TINYINT:
                return true;
            default:
                return false;
        }
    }

    @Override
    public int getColumnDisplaySize(final int column) throws SQLException {
        final int precision = this.column(column).getPrecision();
        return precision > 0 ? precision : 255;
    }

    @Override
    public String getColumnLabel(final int column) throws SQLException {
        final ColumnInfo columnInfo = this.column(column);
        return columnInfo.getColumnLabel() != null ? columnInfo.getColumnLabel() : columnInfo.getColumnName();
    }

    @Override
    public String getColumnName(final int column) throws SQLException {
        return this.column(column).getColumnName();
    }

    @Override
    public String getSchemaName(final int column) throws SQLException {
        final String schemaName = this.column(column).getSchemaName();
        return schemaName == null ? "" : schemaName;
    }

    @Override
    public int getPrecision(final int column) throws SQLException {
        return this.column(column).getPrecision();
    }

    @Override
    public int getScale(final int column) throws SQLException {
        return this.column(column).getScale();
    }

    @Override
    public String getTableName(final int column) throws SQLException {
        final String tableName = this.column(column).getTableName();
        return tableName == null ? "" : tableName;
    }

    @Override
    public String getCatalogName(final int column) throws SQLException {
        this.column(column);
        return "";
    }

    @Override
    public int getColumnType(final int column) throws SQLException {
        return this.column(column).getJdbcType();
    }

    @Override
    public String getColumnTypeName(final int column) throws SQLException {
        final String typeName = this.column(column).getTypeName();
        return typeName == null ? "" : typeName;
    }

    @Override
    public boolean isReadOnly(final int column) throws SQLException {
        this.column(column);
        return true;
    }

    @Override
    public boolean isWritable(final int column) throws SQLException {
        this.column(column);
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(final int column) throws SQLException {
        this.column(column);
        return false;
    }

    @Override
    public String getColumnClassName(final int column) throws SQLException {
        switch (this.column(column).getJdbcType()) {
            case Types.BIGINT:
                return Long.class.getName();
            case Types.BIT:
            case Types.BOOLEAN:
                return Boolean.class.getName();
            case Types.DATE:
                return java.sql.Date.class.getName();
            case Types.DECIMAL:
            case Types.NUMERIC:
                return java.math.BigDecimal.class.getName();
            case Types.DOUBLE:
            case Types.FLOAT:
                return Double.class.getName();
            case Types.INTEGER:
                return Integer.class.getName();
            case Types.REAL:
                return Float.class.getName();
            case Types.SMALLINT:
                return Short.class.getName();
            case Types.TIME:
                return java.sql.Time.class.getName();
            case Types.TIMESTAMP:
                return java.sql.Timestamp.class.getName();
            case Types.TINYINT:
                return Byte.class.getName();
            default:
                return String.class.getName();
        }
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("ResultSetMetaData does not wrap " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) {
        return iface.isInstance(this);
    }

    private ColumnInfo column(final int column) throws SQLException {
        if (column < 1 || column > this.columns.size()) {
            throw new SQLException("Column index out of bounds: " + column);
        }
        return this.columns.get(column - 1);
    }
}
