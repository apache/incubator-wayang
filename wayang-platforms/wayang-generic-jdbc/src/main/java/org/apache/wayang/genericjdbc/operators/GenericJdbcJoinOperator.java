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

package org.apache.wayang.genericjdbc.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.jdbc.operators.JdbcJoinOperator;

/**
 * Generic JDBC implementation of the {@link JoinOperator}.
 */
public class GenericJdbcJoinOperator<KeyType>
        extends JdbcJoinOperator<KeyType>
        implements GenericJdbcExecutionOperator {

    public GenericJdbcJoinOperator(
            TransformationDescriptor<Record, KeyType> keyDescriptor0,
            TransformationDescriptor<Record, KeyType> keyDescriptor1) {
        super(keyDescriptor0, keyDescriptor1);
    }

    public GenericJdbcJoinOperator(JoinOperator<Record, Record, KeyType> that) {
        super(that);
    }

    @Override
    protected GenericJdbcJoinOperator<KeyType> createCopy() {
        return new GenericJdbcJoinOperator<>(this);
    }
}