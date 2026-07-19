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

import org.apache.wayang.jdbc.protocol.MessageEnvelope;
import org.apache.wayang.jdbc.protocol.MessageType;
import org.apache.wayang.jdbc.protocol.io.ProtocolMessageCodec;
import org.apache.wayang.jdbc.protocol.message.CancelQueryRequest;
import org.apache.wayang.jdbc.protocol.message.CancelQueryResponse;
import org.apache.wayang.jdbc.protocol.message.CloseConnectionRequest;
import org.apache.wayang.jdbc.protocol.message.CloseConnectionResponse;
import org.apache.wayang.jdbc.protocol.message.CloseCursorRequest;
import org.apache.wayang.jdbc.protocol.message.CloseCursorResponse;
import org.apache.wayang.jdbc.protocol.message.ErrorCode;
import org.apache.wayang.jdbc.protocol.message.ErrorResponse;
import org.apache.wayang.jdbc.protocol.message.ExecuteQueryRequest;
import org.apache.wayang.jdbc.protocol.message.FetchRequest;
import org.apache.wayang.jdbc.protocol.message.FetchResponse;
import org.apache.wayang.jdbc.protocol.message.GetColumnsRequest;
import org.apache.wayang.jdbc.protocol.message.GetSchemasRequest;
import org.apache.wayang.jdbc.protocol.message.GetTablesRequest;
import org.apache.wayang.jdbc.protocol.message.MetadataResultResponse;
import org.apache.wayang.jdbc.protocol.message.OpenConnectionRequest;
import org.apache.wayang.jdbc.protocol.message.OpenConnectionResponse;
import org.apache.wayang.jdbc.protocol.message.QueryResultResponse;

import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

class WayangJdbcClient implements AutoCloseable {

    private final Socket socket;

    private final ProtocolMessageCodec codec;

    private final String connectionId;

    private boolean closed;

    WayangJdbcClient(final WayangJdbcUrl jdbcUrl) throws SQLException {
        if (jdbcUrl == null) {
            throw new IllegalArgumentException("JDBC URL must not be null.");
        }

        this.codec = new ProtocolMessageCodec();
        this.socket = this.openSocket(jdbcUrl);
        try {
            final Properties properties = jdbcUrl.getProperties();
            final OpenConnectionRequest request = new OpenConnectionRequest(
                    properties.getProperty("user"),
                    jdbcUrl.getDatabase(),
                    this.toStringMap(properties)
            );

            final OpenConnectionResponse response = this.sendAndReceive(
                    MessageType.OPEN_CONNECTION,
                    request,
                    MessageType.OPEN_CONNECTION_OK,
                    OpenConnectionResponse.class
            );
            this.requireText(response.getConnectionId(), "Connection id");
            this.connectionId = response.getConnectionId();
        } catch (SQLException e) {
            this.closed = true;
            try {
                this.socket.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    String getConnectionId() {
        return this.connectionId;
    }

    boolean isClosed() {
        return this.closed || this.socket.isClosed();
    }

    QueryResultResponse executeQuery(
            final String statementId,
            final String sql,
            final int fetchSize
    ) throws SQLException {
        this.requireText(statementId, "Statement id");
        this.requireText(sql, "SQL query");

        return this.sendAndReceive(
                MessageType.EXECUTE_QUERY,
                new ExecuteQueryRequest(this.connectionId, statementId, sql, fetchSize),
                MessageType.QUERY_RESULT,
                QueryResultResponse.class
        );
    }

    FetchResponse fetch(final String cursorId, final int fetchSize) throws SQLException {
        this.requireText(cursorId, "Cursor id");

        return this.sendAndReceive(
                MessageType.FETCH,
                new FetchRequest(this.connectionId, cursorId, fetchSize),
                MessageType.FETCH_RESULT,
                FetchResponse.class
        );
    }

    void closeCursor(final String statementId, final String cursorId) throws SQLException {
        this.requireText(statementId, "Statement id");
        this.requireText(cursorId, "Cursor id");

        this.sendAndReceive(
                MessageType.CLOSE_CURSOR,
                new CloseCursorRequest(this.connectionId, statementId, cursorId),
                MessageType.CLOSE_CURSOR_OK,
                CloseCursorResponse.class
        );
    }

    CancelQueryResponse cancelQuery(final String statementId, final String cursorId) throws SQLException {
        this.requireText(statementId, "Statement id");

        return this.sendAndReceive(
                MessageType.CANCEL_QUERY,
                new CancelQueryRequest(this.connectionId, statementId, cursorId),
                MessageType.CANCEL_QUERY_OK,
                CancelQueryResponse.class
        );
    }

    MetadataResultResponse getSchemas(
            final String catalog,
            final String schemaPattern
    ) throws SQLException {
        return this.sendAndReceive(
                MessageType.GET_SCHEMAS,
                new GetSchemasRequest(this.connectionId, catalog, schemaPattern),
                MessageType.METADATA_RESULT,
                MetadataResultResponse.class
        );
    }

    MetadataResultResponse getTables(
            final String catalog,
            final String schemaPattern,
            final String tableNamePattern,
            final String[] tableTypes
    ) throws SQLException {
        return this.sendAndReceive(
                MessageType.GET_TABLES,
                new GetTablesRequest(
                        this.connectionId,
                        catalog,
                        schemaPattern,
                        tableNamePattern,
                        this.toList(tableTypes)
                ),
                MessageType.METADATA_RESULT,
                MetadataResultResponse.class
        );
    }

    MetadataResultResponse getColumns(
            final String catalog,
            final String schemaPattern,
            final String tableNamePattern,
            final String columnNamePattern
    ) throws SQLException {
        return this.sendAndReceive(
                MessageType.GET_COLUMNS,
                new GetColumnsRequest(
                        this.connectionId,
                        catalog,
                        schemaPattern,
                        tableNamePattern,
                        columnNamePattern
                ),
                MessageType.METADATA_RESULT,
                MetadataResultResponse.class
        );
    }

    @Override
    public void close() throws SQLException {
        if (this.closed) {
            return;
        }

        SQLException failure = null;
        try {
            this.sendAndReceive(
                    MessageType.CLOSE_CONNECTION,
                    new CloseConnectionRequest(this.connectionId),
                    MessageType.CLOSE_CONNECTION_OK,
                    CloseConnectionResponse.class
            );
        } catch (SQLException e) {
            failure = e;
        } finally {
            this.closed = true;
            try {
                this.socket.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = new SQLException("Could not close Wayang JDBC socket.", "08006", e);
                }
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    private Socket openSocket(final WayangJdbcUrl jdbcUrl) throws SQLException {
        try {
            return new Socket(jdbcUrl.getHost(), jdbcUrl.getPort());
        } catch (IOException e) {
            throw new SQLException("Could not connect to Wayang JDBC server.", "08001", e);
        }
    }

    private synchronized <T> T sendAndReceive(
            final MessageType requestType,
            final Object requestPayload,
            final MessageType expectedResponseType,
            final Class<T> responseClass
    ) throws SQLException {
        this.ensureOpen();

        final String requestId = UUID.randomUUID().toString();
        try {
            this.codec.write(
                    this.socket.getOutputStream(),
                    this.codec.toEnvelope(requestId, requestType, requestPayload)
            );

            final MessageEnvelope response = this.codec.read(this.socket.getInputStream());
            if (response == null) {
                throw new SQLException("Wayang JDBC server closed the connection.", "08006");
            }

            if (!requestId.equals(response.getRequestId())) {
                throw new SQLException("Wayang JDBC server returned a mismatched request id.", "HY000");
            }

            if (response.getType() == MessageType.ERROR) {
                throw this.toSqlException(this.codec.payloadAs(response, ErrorResponse.class));
            }

            if (response.getType() != expectedResponseType) {
                throw new SQLException(
                        "Wayang JDBC server returned unexpected response type: " + response.getType(),
                        "HY000"
                );
            }

            return this.codec.payloadAs(response, responseClass);
        } catch (SQLException e) {
            throw e;
        } catch (IOException e) {
            throw new SQLException("Could not communicate with Wayang JDBC server.", "08006", e);
        }
    }

    private SQLException toSqlException(final ErrorResponse response) {
        String message = response.getMessage();

        if (message == null || message.isBlank()) {
            message = "Wayang JDBC server returned an error.";
        }

        if (response.getDetail() != null && !response.getDetail().isBlank()) {
            message = message + " " + response.getDetail();
        }

        if (response.getErrorCode() == ErrorCode.UNSUPPORTED_OPERATION) {
            return new SQLFeatureNotSupportedException(
                    message,
                    response.getSqlState(),
                    response.getVendorCode()
            );
        }

        return new SQLException(
                message,
                response.getSqlState(),
                response.getVendorCode()
        );
    }

    private void ensureOpen() throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("Wayang JDBC connection is closed.", "08003");
        }
    }

    private void requireText(
            final String value,
            final String fieldName
    ) throws SQLException {
        if (value == null || value.isBlank()) {
            throw new SQLException(
                    fieldName + " must not be blank.", "HY000"
            );
        }
    }

    private Map<String, String> toStringMap(final Properties properties) {
        final Map<String, String> values = new LinkedHashMap<>();
        for (String name : properties.stringPropertyNames()) {
            values.put(name, properties.getProperty(name));
        }
        return values;
    }

    private List<String> toList(final String[] values) {
        if (values == null) {
            return Collections.emptyList();
        }
        return Arrays.asList(values);
    }
}
