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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks logical JDBC connections managed by the server.
 */
class JdbcServerSessionManager {

    private final Map<String, JdbcServerSession> sessions = new ConcurrentHashMap<>();

    String openConnection() {
        return this.openConnection(null, null, null);
    }

    String openConnection(
            final String username,
            final String database,
            final Map<String, String> properties
    ) {
        final String connectionId = UUID.randomUUID().toString();
        this.sessions.put(
                connectionId,
                new JdbcServerSession(connectionId, username, database, properties)
        );
        return connectionId;
    }

    boolean isOpen(final String connectionId) {
        return connectionId != null && this.sessions.containsKey(connectionId);
    }

    JdbcServerSession getSession(final String connectionId) {
        return connectionId == null ? null : this.sessions.get(connectionId);
    }

    void closeConnection(final String connectionId) {
        if (connectionId != null) {
            this.sessions.remove(connectionId);
        }
    }
}
