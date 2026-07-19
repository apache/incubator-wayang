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

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.wayang.api.sql.context.SqlQueryResult;
import org.apache.wayang.jdbc.protocol.MessageEnvelope;
import org.apache.wayang.jdbc.protocol.MessageType;
import org.apache.wayang.jdbc.protocol.ProtocolException;
import org.apache.wayang.jdbc.protocol.io.ProtocolMessageCodec;
import org.apache.wayang.jdbc.protocol.message.CancelQueryRequest;
import org.apache.wayang.jdbc.protocol.message.CancelQueryResponse;
import org.apache.wayang.jdbc.protocol.message.CloseConnectionRequest;
import org.apache.wayang.jdbc.protocol.message.CloseConnectionResponse;
import org.apache.wayang.jdbc.protocol.message.CloseCursorRequest;
import org.apache.wayang.jdbc.protocol.message.CloseCursorResponse;
import org.apache.wayang.jdbc.protocol.message.ColumnInfo;
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

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Dispatches protocol messages to server-side JDBC gateway actions.
 */
public class JdbcRequestDispatcher {

    private static final int DEFAULT_FETCH_SIZE = 100;

    private static final String SERVER_VERSION = "1.1.2-SNAPSHOT";

    private final ProtocolMessageCodec codec;

    private final SqlQueryExecutor queryExecutor;

    private final SqlMetadataProvider metadataProvider;

    private final JdbcServerSessionManager sessionManager;

    private final CursorStore cursorStore;

    private final int defaultFetchSize;

    public JdbcRequestDispatcher(final SqlQueryExecutor queryExecutor) {
        this(queryExecutor, DEFAULT_FETCH_SIZE);
    }

    public JdbcRequestDispatcher(final SqlQueryExecutor queryExecutor, final int defaultFetchSize) {
        this(queryExecutor, new DefaultSqlMetadataProvider(), defaultFetchSize);
    }

    public JdbcRequestDispatcher(
            final SqlQueryExecutor queryExecutor,
            final SqlMetadataProvider metadataProvider,
            final int defaultFetchSize
    ) {
        if (queryExecutor == null) {
            throw new IllegalArgumentException("Query executor must not be null.");
        }
        if (metadataProvider == null) {
            throw new IllegalArgumentException("Metadata provider must not be null.");
        }
        if (defaultFetchSize <= 0) {
            throw new IllegalArgumentException("Default fetch size must be positive.");
        }
        this.codec = new ProtocolMessageCodec();
        this.queryExecutor = queryExecutor;
        this.metadataProvider = metadataProvider;
        this.sessionManager = new JdbcServerSessionManager();
        this.cursorStore = new CursorStore();
        this.defaultFetchSize = defaultFetchSize;
    }

    public MessageEnvelope dispatch(final MessageEnvelope request) {
        try {
            switch (request.getType()) {
                case OPEN_CONNECTION:
                    return this.openConnection(request);
                case EXECUTE_QUERY:
                    return this.executeQuery(request);
                case FETCH:
                    return this.fetch(request);
                case CLOSE_CURSOR:
                    return this.closeCursor(request);
                case CANCEL_QUERY:
                    return this.cancelQuery(request);
                case CLOSE_CONNECTION:
                    return this.closeConnection(request);
                case GET_SCHEMAS:
                    return this.getSchemas(request);
                case GET_TABLES:
                    return this.getTables(request);
                case GET_COLUMNS:
                    return this.getColumns(request);
                default:
                    return this.error(
                            request.getRequestId(),
                            ErrorCode.INVALID_REQUEST,
                            "HY000",
                            1000,
                            "Unsupported request message type: " + request.getType(),
                            null,
                            null
                    );
            }
        } catch (ProtocolException | IllegalArgumentException e) {
            return this.error(
                    request.getRequestId(),
                    ErrorCode.INVALID_REQUEST,
                    "HY000",
                    1000,
                    e.getMessage(),
                    null,
                    e.getClass().getName()
            );
        } catch (SqlParseException e) {
            return this.error(
                    request.getRequestId(),
                    ErrorCode.QUERY_PARSE_ERROR,
                    "42000",
                    1002,
                    "Could not parse SQL query.",
                    e.getMessage(),
                    e.getClass().getName()
            );
        } catch (Exception e) {
            return this.error(
                    request.getRequestId(),
                    ErrorCode.QUERY_EXECUTION_ERROR,
                    "HY000",
                    1001,
                    "Could not execute SQL query.",
                    e.getMessage(),
                    e.getClass().getName()
            );
        }
    }

    private MessageEnvelope openConnection(final MessageEnvelope request) throws ProtocolException {
        final OpenConnectionRequest openRequest = this.codec.payloadAs(request, OpenConnectionRequest.class);
        final String connectionId = this.sessionManager.openConnection(
                openRequest.getUsername(),
                openRequest.getDatabase(),
                openRequest.getProperties()
        );
        final OpenConnectionResponse response = new OpenConnectionResponse(
                connectionId,
                SERVER_VERSION,
                new LinkedHashMap<>()
        );
        return this.codec.toEnvelope(request.getRequestId(), MessageType.OPEN_CONNECTION_OK, response);
    }

    private MessageEnvelope executeQuery(final MessageEnvelope request) throws Exception {
        final ExecuteQueryRequest executeRequest = this.codec.payloadAs(request, ExecuteQueryRequest.class);
        this.requireOpenConnection(executeRequest.getConnectionId());
        this.requireText(executeRequest.getStatementId(), "Statement id");
        this.requireText(executeRequest.getSql(), "SQL query");

        final SqlQueryResult result = this.queryExecutor.execute(executeRequest.getSql());
        final List<ColumnInfo> columns = SqlQueryResultAdapter.toColumnInfo(result);
        final List<List<Object>> rows = SqlQueryResultAdapter.toRows(result);
        final int fetchSize = this.normalizeFetchSize(executeRequest.getFetchSize());
        final int batchEnd = Math.min(fetchSize, rows.size());
        final List<List<Object>> firstBatch = List.copyOf(rows.subList(0, batchEnd));
        final boolean hasMoreRows = batchEnd < rows.size();
        final String cursorId = hasMoreRows
                ? this.cursorStore.openCursor(
                        executeRequest.getConnectionId(),
                        executeRequest.getStatementId(),
                        rows,
                        batchEnd
                )
                : null;

        final QueryResultResponse response = new QueryResultResponse(
                executeRequest.getConnectionId(),
                executeRequest.getStatementId(),
                columns,
                firstBatch,
                hasMoreRows,
                cursorId
        );
        return this.codec.toEnvelope(request.getRequestId(), MessageType.QUERY_RESULT, response);
    }

    private MessageEnvelope getSchemas(final MessageEnvelope request) throws SQLException, ProtocolException {
        final GetSchemasRequest metadataRequest = this.codec.payloadAs(request, GetSchemasRequest.class);
        final JdbcServerSession session = this.requireOpenSession(metadataRequest.getConnectionId());
        final MetadataResultResponse response = this.metadataProvider.getSchemas(session, metadataRequest);
        return this.codec.toEnvelope(request.getRequestId(), MessageType.METADATA_RESULT, response);
    }

    private MessageEnvelope getTables(final MessageEnvelope request) throws SQLException, ProtocolException {
        final GetTablesRequest metadataRequest = this.codec.payloadAs(request, GetTablesRequest.class);
        final JdbcServerSession session = this.requireOpenSession(metadataRequest.getConnectionId());
        final MetadataResultResponse response = this.metadataProvider.getTables(session, metadataRequest);
        return this.codec.toEnvelope(request.getRequestId(), MessageType.METADATA_RESULT, response);
    }

    private MessageEnvelope getColumns(final MessageEnvelope request) throws SQLException, ProtocolException {
        final GetColumnsRequest metadataRequest = this.codec.payloadAs(request, GetColumnsRequest.class);
        final JdbcServerSession session = this.requireOpenSession(metadataRequest.getConnectionId());
        final MetadataResultResponse response = this.metadataProvider.getColumns(session, metadataRequest);
        return this.codec.toEnvelope(request.getRequestId(), MessageType.METADATA_RESULT, response);
    }

    private MessageEnvelope fetch(final MessageEnvelope request) throws ProtocolException {
        final FetchRequest fetchRequest = this.codec.payloadAs(request, FetchRequest.class);
        this.requireOpenConnection(fetchRequest.getConnectionId());
        this.requireText(fetchRequest.getCursorId(), "Cursor id");

        final CursorStore.FetchBatch batch = this.cursorStore.fetch(
                fetchRequest.getConnectionId(),
                fetchRequest.getCursorId(),
                this.normalizeFetchSize(fetchRequest.getFetchSize())
        );
        if (batch == null) {
            throw new IllegalArgumentException("Unknown cursor id: " + fetchRequest.getCursorId());
        }

        final FetchResponse response = new FetchResponse(
                fetchRequest.getConnectionId(),
                fetchRequest.getCursorId(),
                batch.getRows(),
                batch.hasMoreRows()
        );
        return this.codec.toEnvelope(request.getRequestId(), MessageType.FETCH_RESULT, response);
    }

    private MessageEnvelope closeCursor(final MessageEnvelope request) throws ProtocolException {
        final CloseCursorRequest closeRequest = this.codec.payloadAs(request, CloseCursorRequest.class);
        this.requireOpenConnection(closeRequest.getConnectionId());
        this.requireText(closeRequest.getStatementId(), "Statement id");
        this.requireText(closeRequest.getCursorId(), "Cursor id");
        this.cursorStore.closeCursor(closeRequest.getConnectionId(), closeRequest.getCursorId());

        final CloseCursorResponse response = new CloseCursorResponse(
                closeRequest.getConnectionId(),
                closeRequest.getStatementId(),
                closeRequest.getCursorId()
        );
        return this.codec.toEnvelope(request.getRequestId(), MessageType.CLOSE_CURSOR_OK, response);
    }

    private MessageEnvelope cancelQuery(final MessageEnvelope request) throws ProtocolException {
        final CancelQueryRequest cancelRequest = this.codec.payloadAs(request, CancelQueryRequest.class);
        this.requireOpenConnection(cancelRequest.getConnectionId());
        this.requireText(cancelRequest.getStatementId(), "Statement id");
        final boolean cancelled = cancelRequest.getCursorId() != null
                && this.cursorStore.cancelCursor(cancelRequest.getConnectionId(), cancelRequest.getCursorId());

        final CancelQueryResponse response = new CancelQueryResponse(
                cancelRequest.getConnectionId(),
                cancelRequest.getStatementId(),
                cancelRequest.getCursorId(),
                cancelled
        );
        return this.codec.toEnvelope(request.getRequestId(), MessageType.CANCEL_QUERY_OK, response);
    }

    private MessageEnvelope closeConnection(final MessageEnvelope request) throws ProtocolException {
        final CloseConnectionRequest closeRequest = this.codec.payloadAs(request, CloseConnectionRequest.class);
        this.requireOpenConnection(closeRequest.getConnectionId());
        this.cursorStore.closeConnection(closeRequest.getConnectionId());
        this.sessionManager.closeConnection(closeRequest.getConnectionId());

        final CloseConnectionResponse response = new CloseConnectionResponse(closeRequest.getConnectionId());
        return this.codec.toEnvelope(request.getRequestId(), MessageType.CLOSE_CONNECTION_OK, response);
    }

    private void requireOpenConnection(final String connectionId) {
        this.requireOpenSession(connectionId);
    }

    private JdbcServerSession requireOpenSession(final String connectionId) {
        this.requireText(connectionId, "Connection id");
        final JdbcServerSession session = this.sessionManager.getSession(connectionId);
        if (session == null) {
            throw new IllegalArgumentException("Unknown connection id: " + connectionId);
        }
        return session;
    }

    private void requireText(final String value, final String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank.");
        }
    }

    private int normalizeFetchSize(final int fetchSize) {
        return fetchSize > 0 ? fetchSize : this.defaultFetchSize;
    }

    private MessageEnvelope error(
            final String requestId,
            final ErrorCode errorCode,
            final String sqlState,
            final int vendorCode,
            final String message,
            final String detail,
            final String exceptionClass
    ) {
        final ErrorResponse response = new ErrorResponse(
                errorCode,
                sqlState,
                vendorCode,
                message,
                detail,
                exceptionClass
        );
        return this.codec.toEnvelope(requestId, MessageType.ERROR, response);
    }
}
