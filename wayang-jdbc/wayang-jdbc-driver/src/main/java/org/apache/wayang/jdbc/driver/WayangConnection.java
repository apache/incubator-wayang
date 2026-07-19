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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

class WayangConnection implements Connection {

    private final WayangJdbcClient client;

    private final String jdbcUrl;

    private final String userName;

    private final Properties clientInfo = new Properties();

    private boolean closed;
    private boolean readOnly = true;
    private boolean autoCommit = true;
    private String catalog;
    private String schema;

    WayangConnection(final WayangJdbcClient client, final String database) {
        this(client, database, null, null);
    }

    WayangConnection(
            final WayangJdbcClient client,
            final String database,
            final String jdbcUrl,
            final Properties properties
    ) {
        if (client == null) {
            throw new IllegalArgumentException("JDBC client must not be null.");
        }
        this.client = client;
        this.jdbcUrl = jdbcUrl;
        this.userName = properties == null ? null : properties.getProperty("user");
        this.catalog = database;
        this.schema = database;
    }

    @Override
    public Statement createStatement() throws SQLException {
        this.ensureOpen();
        return new WayangStatement(this, this.client);
    }

    @Override
    public void close() throws SQLException {
        if (this.closed) {
            return;
        }
        try {
            this.client.close();
        } finally {
            this.closed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return this.closed || this.client.isClosed();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        this.ensureOpen();
        return this.autoCommit;
    }

    @Override
    public void setAutoCommit(final boolean autoCommit) throws SQLException {
        this.ensureOpen();
        if (!autoCommit) {
            throw this.unsupported("Wayang JDBC does not support transactions.");
        }
        this.autoCommit = true;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        this.ensureOpen();
        return this.readOnly;
    }

    @Override
    public void setReadOnly(final boolean readOnly) throws SQLException {
        this.ensureOpen();
        if (!readOnly) {
            throw this.unsupported("Wayang JDBC connections are read-only.");
        }
        this.readOnly = true;
    }

    @Override
    public String getCatalog() throws SQLException {
        this.ensureOpen();
        return this.catalog;
    }

    @Override
    public void setCatalog(final String catalog) throws SQLException {
        this.ensureOpen();
        this.catalog = catalog;
    }

    @Override
    public String getSchema() throws SQLException {
        this.ensureOpen();
        return this.schema;
    }

    @Override
    public void setSchema(final String schema) throws SQLException {
        this.ensureOpen();
        this.schema = schema;
    }

    @Override
    public boolean isValid(final int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException("Timeout must not be negative.");
        }
        return !this.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        this.ensureOpen();
        return WayangDatabaseMetaData.create(this);
    }

    @Override
    public void commit() throws SQLException {
        throw this.unsupported("Transactions are not supported.");
    }

    @Override
    public void rollback() throws SQLException {
        throw this.unsupported("Transactions are not supported.");
    }

    @Override
    public void rollback(final Savepoint savepoint) throws SQLException {
        throw this.unsupported("Savepoints are not supported.");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw this.unsupported("Savepoints are not supported.");
    }

    @Override
    public Savepoint setSavepoint(final String name) throws SQLException {
        throw this.unsupported("Savepoints are not supported.");
    }

    @Override
    public void releaseSavepoint(final Savepoint savepoint) throws SQLException {
        throw this.unsupported("Savepoints are not supported.");
    }

    @Override
    public PreparedStatement prepareStatement(final String sql) throws SQLException {
        throw this.unsupported("Prepared statements are not supported yet.");
    }

    @Override
    public CallableStatement prepareCall(final String sql) throws SQLException {
        throw this.unsupported("Callable statements are not supported.");
    }

    @Override
    public String nativeSQL(final String sql) throws SQLException {
        this.ensureOpen();
        return sql;
    }

    @Override
    public Statement createStatement(final int type, final int concurrency) throws SQLException {
        this.ensureOpen();
        this.requireSupportedResultSetOptions(type, concurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
        return this.createStatement();
    }

    @Override
    public Statement createStatement(final int type, final int concurrency, final int holdability) throws SQLException {
        this.ensureOpen();
        this.requireSupportedResultSetOptions(type, concurrency, holdability);
        return this.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int type, final int concurrency) throws SQLException {
        throw this.unsupported("Prepared statements are not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int type, final int concurrency, final int holdability) throws SQLException {
        throw this.unsupported("Prepared statements are not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys) throws SQLException {
        throw this.unsupported("Generated keys are not supported.");
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int[] columnIndexes) throws SQLException {
        throw this.unsupported("Generated keys are not supported.");
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final String[] columnNames) throws SQLException {
        throw this.unsupported("Generated keys are not supported.");
    }

    @Override
    public CallableStatement prepareCall(final String sql, final int type, final int concurrency) throws SQLException {
        throw this.unsupported("Callable statements are not supported.");
    }

    @Override
    public CallableStatement prepareCall(final String sql, final int type, final int concurrency, final int holdability) throws SQLException {
        throw this.unsupported("Callable statements are not supported.");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        this.ensureOpen();
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public void setTransactionIsolation(final int level) throws SQLException {
        this.ensureOpen();
        if (level != Connection.TRANSACTION_NONE) {
            throw this.unsupported("Transactions are not supported.");
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        this.ensureOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.ensureOpen();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        this.ensureOpen();
        return Collections.emptyMap();
    }

    @Override
    public void setTypeMap(final Map<String, Class<?>> map) throws SQLException {
        throw this.unsupported("Custom type maps are not supported.");
    }

    @Override
    public int getHoldability() throws SQLException {
        this.ensureOpen();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public void setHoldability(final int holdability) throws SQLException {
        this.ensureOpen();
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw this.unsupported("Wayang JDBC supports only close-cursors-at-commit holdability.");
        }
    }

    @Override
    public Clob createClob() throws SQLException {
        throw this.unsupported("CLOB values are not supported.");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw this.unsupported("BLOB values are not supported.");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw this.unsupported("NCLOB values are not supported.");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw this.unsupported("SQLXML values are not supported.");
    }

    @Override
    public Array createArrayOf(final String typeName, final Object[] elements) throws SQLException {
        throw this.unsupported("SQL ARRAY values are not supported.");
    }

    @Override
    public Struct createStruct(final String typeName, final Object[] attributes) throws SQLException {
        throw this.unsupported("SQL STRUCT values are not supported.");
    }

    @Override
    public void setClientInfo(final String name, final String value) {
        this.clientInfo.setProperty(name, value);
    }

    @Override
    public String getClientInfo(final String name) {
        return this.clientInfo.getProperty(name);
    }

    @Override
    public Properties getClientInfo() {
        final Properties copy = new Properties();
        copy.putAll(this.clientInfo);
        return copy;
    }

    @Override
    public void setClientInfo(final Properties properties) {
        this.clientInfo.putAll(properties);
    }

    @Override
    public void abort(final Executor executor) throws SQLException {
        if (executor == null) {
            throw new SQLException("Executor must not be null.");
        }
        executor.execute(() -> {
            try {
                close();
            } catch (SQLException ignored) {
            }
        });
    }

    @Override
    public void setNetworkTimeout(final Executor executor, final int milliseconds) throws SQLException {
        throw this.unsupported("Network timeout is not supported.");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        this.ensureOpen();
        return 0;
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Connection does not wrap " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) {
        return iface.isInstance(this);
    }

    String getJdbcUrl() {
        return this.jdbcUrl;
    }

    String getUserName() {
        return this.userName;
    }

    WayangJdbcClient getClient() {
        return this.client;
    }

    private void ensureOpen() throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("Wayang JDBC connection is closed.", "08003");
        }
    }

    private void requireSupportedResultSetOptions(
            final int type,
            final int concurrency,
            final int holdability
    ) throws SQLException {
        if (type != ResultSet.TYPE_FORWARD_ONLY) {
            throw this.unsupported("Wayang JDBC supports only forward-only result sets.");
        }
        if (concurrency != ResultSet.CONCUR_READ_ONLY) {
            throw this.unsupported("Wayang JDBC supports only read-only result sets.");
        }
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw this.unsupported("Wayang JDBC supports only close-cursors-at-commit holdability.");
        }
    }

    private SQLFeatureNotSupportedException unsupported(final String message) {
        return new SQLFeatureNotSupportedException(message, "0A000");
    }
}
