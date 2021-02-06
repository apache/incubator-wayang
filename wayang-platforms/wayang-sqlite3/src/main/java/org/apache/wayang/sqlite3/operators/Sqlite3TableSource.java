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

import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.jdbc.operators.JdbcTableSource;
import org.apache.wayang.sqlite3.platform.Sqlite3Platform;

import java.util.List;

/**
 * Implementation of the {@link TableSource} for the {@link Sqlite3Platform}.
 */
public class Sqlite3TableSource extends JdbcTableSource {

    /**
     * Creates a new instance.
     *
     * @see TableSource#TableSource(String, String...)
     */
    public Sqlite3TableSource(String tableName, String... columnNames) {
        super(tableName, columnNames);
    }

    public Sqlite3TableSource(Sqlite3TableSource that) {
        super(that);
    }

    @Override
    public Sqlite3Platform getPlatform() {
        return Sqlite3Platform.getInstance();
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException("Input channels are not supported.");
    }

}
