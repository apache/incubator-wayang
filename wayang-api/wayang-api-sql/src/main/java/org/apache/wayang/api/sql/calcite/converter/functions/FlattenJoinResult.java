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

package org.apache.wayang.api.sql.calcite.converter.functions;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.function.FunctionDescriptor;

/**
 * Flattens the result of a join i.e. a {@link Tuple2} of a left and a right
 * {@link Record} to a single {@link Record}.
 */
public class FlattenJoinResult implements FunctionDescriptor.SerializableFunction<Tuple2<Record, Record>, Record> {

    @Override
    public Record apply(final Tuple2<Record, Record> tuple2) {
        final int length0 = tuple2.getField0().size();
        final int length1 = tuple2.getField1().size();

        final int totalLength = length0 + length1;

        final Object[] fields = new Object[totalLength];

        for (int i = 0; i < length0; i++) {
            fields[i] = tuple2.getField0().getField(i);
        }

        for (int i = length0; i < totalLength; i++) {
            fields[i] = tuple2.getField1().getField(i - length0);
        }

        return new Record(fields);
    }
}
