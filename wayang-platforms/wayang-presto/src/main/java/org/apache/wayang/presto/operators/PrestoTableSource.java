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

package org.apache.wayang.presto.operators;

import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.jdbc.operators.JdbcTableSource;

import java.util.List;

/**
 * Presto implementation of the {@link TableSource}.
 *
 * <p>Table names follow Presto's three-part convention
 * {@code catalog.schema.table} (e.g. {@code memory.sales.orders}).
 */
public class PrestoTableSource extends JdbcTableSource implements PrestoExecutionOperator {

    /**
     * @see TableSource#TableSource(String, String...)
     */
    public PrestoTableSource(String tableName, String... columnNames) {
        super(tableName, columnNames);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public PrestoTableSource(JdbcTableSource that) {
        super(that);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no input channels.");
    }
}
