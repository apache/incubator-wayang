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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores in-memory row cursors for query results that need fetch paging.
 */
class CursorStore {

    private final Map<String, Cursor> cursors = new ConcurrentHashMap<>();

    String openCursor(
            final String connectionId,
            final String statementId,
            final List<List<Object>> rows,
            final int position
    ) {
        final String cursorId = UUID.randomUUID().toString();
        this.cursors.put(cursorId, new Cursor(connectionId, statementId, rows, position));
        return cursorId;
    }

    FetchBatch fetch(final String connectionId, final String cursorId, final int fetchSize) {
        final Cursor cursor = this.cursors.get(cursorId);
        if (cursor == null || !cursor.connectionId.equals(connectionId)) {
            return null;
        }

        final FetchBatch batch = cursor.fetch(fetchSize);
        if (!batch.hasMoreRows()) {
            this.cursors.remove(cursorId);
        }
        return batch;
    }

    boolean closeCursor(final String connectionId, final String cursorId) {
        final Cursor cursor = this.cursors.get(cursorId);
        if (cursor == null || !cursor.connectionId.equals(connectionId)) {
            return false;
        }
        return this.cursors.remove(cursorId) != null;
    }

    boolean cancelCursor(final String connectionId, final String cursorId) {
        return this.closeCursor(connectionId, cursorId);
    }

    void closeConnection(final String connectionId) {
        final Iterator<Map.Entry<String, Cursor>> iterator = this.cursors.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<String, Cursor> entry = iterator.next();
            if (entry.getValue().connectionId.equals(connectionId)) {
                iterator.remove();
            }
        }
    }

    static class FetchBatch {

        private final List<List<Object>> rows;

        private final boolean hasMoreRows;

        FetchBatch(final List<List<Object>> rows, final boolean hasMoreRows) {
            this.rows = rows;
            this.hasMoreRows = hasMoreRows;
        }

        List<List<Object>> getRows() {
            return this.rows;
        }

        boolean hasMoreRows() {
            return this.hasMoreRows;
        }
    }

    private static class Cursor {

        private final String connectionId;

        private final String statementId;

        private final List<List<Object>> rows;

        private int position;

        Cursor(
                final String connectionId,
                final String statementId,
                final List<List<Object>> rows,
                final int position
        ) {
            this.connectionId = connectionId;
            this.statementId = statementId;
            this.rows = rows;
            this.position = position;
        }

        synchronized FetchBatch fetch(final int fetchSize) {
            final int end = Math.min(this.position + fetchSize, this.rows.size());
            final List<List<Object>> batch = new ArrayList<>(this.rows.subList(this.position, end));
            this.position = end;
            return new FetchBatch(batch, this.position < this.rows.size());
        }
    }
}
