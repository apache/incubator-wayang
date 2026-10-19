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

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.jdbc.operators.JdbcJoinOperator;

/**
 * Presto implementation of the {@link JoinOperator}. The two key descriptors
 * carry the {@code (table, keyColumns)} SQL implementation that the base class
 * renders into a {@code JOIN ... ON ...} clause.
 *
 * @param <KeyType> type of the join key
 */
public class PrestoJoinOperator<KeyType> extends JdbcJoinOperator<KeyType> implements PrestoExecutionOperator {

    public PrestoJoinOperator(
            TransformationDescriptor<Record, KeyType> keyDescriptor0,
            TransformationDescriptor<Record, KeyType> keyDescriptor1) {
        super(keyDescriptor0, keyDescriptor1);
    }

    public PrestoJoinOperator(JoinOperator<Record, Record, KeyType> that) {
        super(that);
    }

    @Override
    protected PrestoJoinOperator<KeyType> createCopy() {
        return new PrestoJoinOperator<KeyType>(this);
    }
}
