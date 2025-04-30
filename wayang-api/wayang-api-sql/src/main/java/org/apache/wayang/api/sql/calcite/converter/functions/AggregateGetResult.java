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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class AggregateGetResult implements FunctionDescriptor.SerializableFunction<Record, Record> {
    private final List<AggregateCall> aggregateCallList;
    private final Set<Integer> groupingfields;

    public AggregateGetResult(final List<AggregateCall> aggregateCalls, final Set<Integer> groupingfields) {
        this.aggregateCallList = aggregateCalls;
        this.groupingfields = groupingfields;
    }

    @Override
    public Record apply(final Record record) {
        final int recordSize = record.size();
        final int aggregateCallOffset = recordSize - aggregateCallList.size() - 1;

        final Object[] fields = groupingfields.stream()
                .map(record::getField)
                .toArray();

        final Object[] aggregateCallFields = IntStream.range(0, aggregateCallList.size())
                .mapToObj(i -> aggregateCallList.get(i).getAggregation().getKind().equals(SqlKind.AVG)
                        ? record.getDouble(i + aggregateCallOffset) / record.getDouble(recordSize - 1)
                        : record.getField(i + aggregateCallOffset))
                .toArray();

        final Object[] combinedFields = Stream.concat(Arrays.stream(fields), Arrays.stream(aggregateCallFields))
                .toArray();
                
        return new Record(combinedFields);
    }
}