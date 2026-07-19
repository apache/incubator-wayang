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
import org.apache.wayang.jdbc.protocol.message.FetchResponse;
import org.apache.wayang.jdbc.protocol.message.QueryResultResponse;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

final class WayangResultSet implements InvocationHandler {

    private final WayangStatement statement;

    private final WayangJdbcClient client;

    private final List<ColumnInfo> columns;

    private final ResultSetMetaData metaData;

    private final int fetchSize;

    private final int maxRows;

    private List<List<Object>> rows;

    private String cursorId;

    private boolean hasMoreRows;

    private int rowIndex = -1;

    private int rowNumber;

    private boolean afterLast;

    private boolean closed;

    private Object lastValue;

    private WayangResultSet(
            final WayangStatement statement,
            final WayangJdbcClient client,
            final QueryResultResponse response,
            final int fetchSize,
            final int maxRows
    ) {
        this.statement = statement;
        this.client = client;
        this.columns = List.copyOf(response.getColumns());
        this.rows = this.copyRows(response.getRows());
        this.cursorId = response.getCursorId();
        this.hasMoreRows = response.isHasMoreRows();
        this.fetchSize = fetchSize;
        this.maxRows = maxRows;
        this.metaData = new WayangResultSetMetaData(this.columns);
    }

    static ResultSet create(
            final WayangStatement statement,
            final WayangJdbcClient client,
            final QueryResultResponse response,
            final int fetchSize,
            final int maxRows
    ) {
        return (ResultSet) Proxy.newProxyInstance(
                ResultSet.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                new WayangResultSet(statement, client, response, fetchSize, maxRows)
        );
    }

    @Override
    public Object invoke(
            final Object proxy,
            final Method method,
            final Object[] args
    ) throws Throwable {
        if (method.getDeclaringClass() == Object.class) {
            return this.invokeObjectMethod(proxy, method, args);
        }

        final String methodName = method.getName();
        switch (methodName) {
            case "next":
                return this.next();
            case "close":
                this.close((ResultSet) proxy);
                return null;
            case "isClosed":
                return this.closed;
            case "wasNull":
                this.ensureOpen();
                return this.lastValue == null;
            case "getMetaData":
                this.ensureOpen();
                return this.metaData;
            case "getStatement":
                this.ensureOpen();
                return this.statement;
            case "findColumn":
                this.ensureOpen();
                return this.findColumn((String) args[0]);
            case "getObject":
                return this.getObject(args);
            case "getString":
            case "getNString":
                return this.asString(this.value(args[0]));
            case "getBoolean":
                return this.asBoolean(this.value(args[0]));
            case "getByte":
                return this.asNumber(this.value(args[0])).byteValue();
            case "getShort":
                return this.asNumber(this.value(args[0])).shortValue();
            case "getInt":
                return this.asNumber(this.value(args[0])).intValue();
            case "getLong":
                return this.asNumber(this.value(args[0])).longValue();
            case "getFloat":
                return this.asNumber(this.value(args[0])).floatValue();
            case "getDouble":
                return this.asNumber(this.value(args[0])).doubleValue();
            case "getBigDecimal":
                return this.asBigDecimal(this.value(args[0]));
            case "getBytes":
                return this.asBytes(this.value(args[0]));
            case "getAsciiStream":
            case "getBinaryStream":
            case "getUnicodeStream":
                return this.asBinaryStream(this.value(args[0]));
            case "getCharacterStream":
            case "getNCharacterStream":
                return this.asCharacterStream(this.value(args[0]));
            case "getDate":
                return this.asDate(this.value(args[0]));
            case "getTime":
                return this.asTime(this.value(args[0]));
            case "getTimestamp":
                return this.asTimestamp(this.value(args[0]));
            case "getURL":
                return this.asUrl(this.value(args[0]));
            case "getArray":
                return this.unsupportedValue("SQL ARRAY values are not supported.");
            case "getBlob":
                return this.unsupportedValue("BLOB values are not supported.");
            case "getClob":
                return this.unsupportedValue("CLOB values are not supported.");
            case "getNClob":
                return this.unsupportedValue("NCLOB values are not supported.");
            case "getSQLXML":
                return this.unsupportedValue("SQLXML values are not supported.");
            case "getRow":
                this.ensureOpen();
                return this.rowIndex >= 0 && this.rowIndex < this.rows.size() ? this.rowNumber : 0;
            case "isBeforeFirst":
                this.ensureOpen();
                return this.rowNumber == 0 && !this.afterLast;
            case "isAfterLast":
                this.ensureOpen();
                return this.afterLast;
            case "isFirst":
                this.ensureOpen();
                return this.rowNumber == 1 && this.rowIndex >= 0;
            case "isLast":
                this.ensureOpen();
                return !this.hasMoreRows && this.rowIndex == this.rows.size() - 1 && this.rowIndex >= 0;
            case "rowUpdated":
            case "rowInserted":
            case "rowDeleted":
                this.ensureOpen();
                return false;
            case "beforeFirst":
            case "afterLast":
            case "first":
            case "last":
            case "absolute":
            case "relative":
            case "previous":
                throw this.unsupported("Wayang JDBC supports only forward-only result sets.");
            case "getType":
                this.ensureOpen();
                return ResultSet.TYPE_FORWARD_ONLY;
            case "getConcurrency":
                this.ensureOpen();
                return ResultSet.CONCUR_READ_ONLY;
            case "getHoldability":
                this.ensureOpen();
                return ResultSet.CLOSE_CURSORS_AT_COMMIT;
            case "getFetchDirection":
                this.ensureOpen();
                return ResultSet.FETCH_FORWARD;
            case "setFetchDirection":
                this.ensureOpen();
                if ((Integer) args[0] != ResultSet.FETCH_FORWARD) {
                    throw this.unsupported("Wayang JDBC supports only forward fetch direction.");
                }
                return null;
            case "getFetchSize":
                this.ensureOpen();
                return this.fetchSize;
            case "setFetchSize":
                this.ensureOpen();
                if ((Integer) args[0] < 0) {
                    throw new SQLException("Fetch size must not be negative.");
                }
                return null;
            case "getWarnings":
                this.ensureOpen();
                return null;
            case "clearWarnings":
                this.ensureOpen();
                return null;
            case "unwrap":
                return this.unwrap(proxy, (Class<?>) args[0]);
            case "isWrapperFor":
                return ((Class<?>) args[0]).isInstance(proxy);
            default:
                throw this.unsupported("ResultSet method is not supported yet: " + methodName);
        }
    }

    private Object invokeObjectMethod(
            final Object proxy,
            final Method method,
            final Object[] args
    ) {
        switch (method.getName()) {
            case "toString":
                return "WayangResultSet";
            case "hashCode":
                return System.identityHashCode(proxy);
            case "equals":
                return proxy == args[0];
            default:
                throw new IllegalStateException("Unsupported Object method: " + method.getName());
        }
    }

    private boolean next() throws SQLException {
        this.ensureOpen();
        this.lastValue = null;

        if (this.reachedMaxRows()) {
            this.closeCursorIfOpen();
            this.afterLast = true;
            return false;
        }

        if (this.rowIndex + 1 < this.rows.size()) {
            this.rowIndex++;
            this.rowNumber++;
            return true;
        }

        while (this.hasMoreRows && this.cursorId != null) {
            final FetchResponse response = this.client.fetch(this.cursorId, this.fetchSize);
            this.rows = this.copyRows(response.getRows());
            this.hasMoreRows = response.isHasMoreRows();
            if (!this.hasMoreRows) {
                this.cursorId = null;
            }
            this.rowIndex = -1;

            if (this.reachedMaxRows()) {
                this.closeCursorIfOpen();
                this.afterLast = true;
                return false;
            }

            if (this.rowIndex + 1 < this.rows.size()) {
                this.rowIndex++;
                this.rowNumber++;
                return true;
            }
        }

        this.afterLast = true;
        return false;
    }

    private Object getObject(final Object[] args) throws SQLException {
        if (args.length == 1) {
            return this.value(args[0]);
        }

        final Object value = this.value(args[0]);
        if (!(args[1] instanceof Class<?>)) {
            return value;
        }

        final Class<?> targetType = (Class<?>) args[1];
        if (value == null || targetType.isInstance(value)) {
            return value;
        }
        if (targetType == String.class) {
            return this.asString(value);
        }
        if (targetType == Boolean.class || targetType == boolean.class) {
            return this.asBoolean(value);
        }
        if (targetType == Byte.class || targetType == byte.class) {
            return this.asNumber(value).byteValue();
        }
        if (targetType == Short.class || targetType == short.class) {
            return this.asNumber(value).shortValue();
        }
        if (targetType == Integer.class || targetType == int.class) {
            return this.asNumber(value).intValue();
        }
        if (targetType == Long.class || targetType == long.class) {
            return this.asNumber(value).longValue();
        }
        if (targetType == Float.class || targetType == float.class) {
            return this.asNumber(value).floatValue();
        }
        if (targetType == Double.class || targetType == double.class) {
            return this.asNumber(value).doubleValue();
        }
        if (targetType == BigDecimal.class) {
            return this.asBigDecimal(value);
        }
        throw new SQLException("Cannot convert value to " + targetType.getName());
    }

    private Object value(final Object column) throws SQLException {
        this.ensureOpen();
        final int columnIndex;
        if (column instanceof Integer) {
            columnIndex = (Integer) column;
        } else if (column instanceof String) {
            columnIndex = this.findColumn((String) column);
        } else {
            throw new SQLException("Unsupported column reference: " + column);
        }

        final List<Object> row = this.currentRow();
        if (columnIndex < 1 || columnIndex > this.columns.size()) {
            throw new SQLException("Column index out of bounds: " + columnIndex);
        }
        this.lastValue = columnIndex <= row.size() ? row.get(columnIndex - 1) : null;
        return this.lastValue;
    }

    private List<Object> currentRow() throws SQLException {
        if (this.rowIndex < 0 || this.rowIndex >= this.rows.size()) {
            throw new SQLException("ResultSet cursor is not positioned on a row.");
        }
        return this.rows.get(this.rowIndex);
    }

    private int findColumn(final String columnLabel) throws SQLException {
        if (columnLabel == null || columnLabel.isBlank()) {
            throw new SQLException("Column label must not be blank.");
        }
        for (int i = 0; i < this.columns.size(); i++) {
            final ColumnInfo column = this.columns.get(i);
            if (columnLabel.equalsIgnoreCase(column.getColumnLabel())
                    || columnLabel.equalsIgnoreCase(column.getColumnName())) {
                return i + 1;
            }
        }
        throw new SQLException("Unknown column label: " + columnLabel);
    }

    private void close(final ResultSet proxy) throws SQLException {
        if (this.closed) {
            return;
        }

        SQLException failure = null;
        try {
            this.closeCursorIfOpen();
        } catch (SQLException e) {
            failure = e;
        } finally {
            this.closed = true;
            this.statement.resultSetClosed(proxy);
        }

        if (failure != null) {
            throw failure;
        }
    }

    private List<List<Object>> copyRows(final List<List<Object>> sourceRows) {
        final List<List<Object>> copy = new ArrayList<>();
        if (sourceRows == null) {
            return copy;
        }
        for (List<Object> row : sourceRows) {
            copy.add(new ArrayList<>(row));
        }
        return copy;
    }

    private boolean reachedMaxRows() {
        return this.maxRows > 0 && this.rowNumber >= this.maxRows;
    }

    private void closeCursorIfOpen() throws SQLException {
        if (this.cursorId != null) {
            this.client.closeCursor(this.statement.getStatementId(), this.cursorId);
            this.cursorId = null;
            this.hasMoreRows = false;
        }
    }

    private String asString(final Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private Boolean asBoolean(final Object value) throws SQLException {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue() != 0D;
        }
        final String text = String.valueOf(value).trim().toLowerCase(Locale.ROOT);
        if ("true".equals(text) || "1".equals(text)) {
            return true;
        }
        if ("false".equals(text) || "0".equals(text)) {
            return false;
        }
        throw new SQLException("Cannot convert value to boolean: " + value);
    }

    private Number asNumber(final Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return (Number) value;
        }
        if (value instanceof Boolean) {
            return (Boolean) value ? 1 : 0;
        }
        try {
            return new BigDecimal(String.valueOf(value));
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert value to number: " + value, e);
        }
    }

    private BigDecimal asBigDecimal(final Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number || value instanceof Boolean || value instanceof String) {
            return new BigDecimal(String.valueOf(this.asNumber(value)));
        }
        throw new SQLException("Cannot convert value to BigDecimal: " + value);
    }

    private byte[] asBytes(final Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }

    private java.io.InputStream asBinaryStream(final Object value) throws SQLException {
        final byte[] bytes = this.asBytes(value);
        return bytes == null ? null : new java.io.ByteArrayInputStream(bytes);
    }

    private java.io.Reader asCharacterStream(final Object value) {
        final String text = this.asString(value);
        return text == null ? null : new java.io.StringReader(text);
    }

    private Date asDate(final Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return (Date) value;
        }
        try {
            return Date.valueOf(String.valueOf(value));
        } catch (IllegalArgumentException e) {
            throw new SQLException("Cannot convert value to Date: " + value, e);
        }
    }

    private Time asTime(final Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Time) {
            return (Time) value;
        }
        try {
            return Time.valueOf(String.valueOf(value));
        } catch (IllegalArgumentException e) {
            throw new SQLException("Cannot convert value to Time: " + value, e);
        }
    }

    private Timestamp asTimestamp(final Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }
        try {
            return Timestamp.valueOf(String.valueOf(value));
        } catch (IllegalArgumentException e) {
            throw new SQLException("Cannot convert value to Timestamp: " + value, e);
        }
    }

    private URL asUrl(final Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof URL) {
            return (URL) value;
        }
        try {
            return new URL(String.valueOf(value));
        } catch (MalformedURLException e) {
            throw new SQLException("Cannot convert value to URL: " + value, e);
        }
    }

    private Object unsupportedValue(final String message) throws SQLFeatureNotSupportedException {
        throw this.unsupported(message);
    }

    private Object unwrap(final Object proxy, final Class<?> iface) throws SQLException {
        if (iface.isInstance(proxy)) {
            return iface.cast(proxy);
        }
        throw new SQLException("ResultSet does not wrap " + iface.getName());
    }

    private void ensureOpen() throws SQLException {
        if (this.closed) {
            throw new SQLException("Wayang JDBC result set is closed.", "24000");
        }
    }

    private SQLFeatureNotSupportedException unsupported(final String message) {
        return new SQLFeatureNotSupportedException(message, "0A000");
    }
}
