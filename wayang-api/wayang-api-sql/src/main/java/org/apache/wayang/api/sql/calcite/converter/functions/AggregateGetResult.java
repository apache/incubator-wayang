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

import org.apache.calcite.rel.core.AggregateCall;
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
        System.out.println("GetResult: rec: " + record);
        System.out.println("GetResult: aggCall: " + aggregateCallList);

        final int recordSize = record.size();
        final int outputRecordSize = aggregateCallList.size() + groupingfields.size();
        final Object[] resValues = new Object[outputRecordSize];

        int i = 0;
        int j = 0;
        
        groupingfields.stream().forEach(System.out::println);

        System.out.println("GetResult: grouping fields: " + groupingfields);
        for (i = 0; j < groupingfields.size(); i++) {
            if (groupingfields.contains(i)) {
                resValues[j] = record.getField(i);
                j++;
            }
        }

        System.out.println("GetResult: resvalues post calc: " + Arrays.toString(resValues));
        
        i = recordSize - aggregateCallList.size() - 1;
        for (final AggregateCall aggregateCall : aggregateCallList) {
            final String name = aggregateCall.getAggregation().getName();
            if (name.equals("AVG")) {
                System.out.println("GetResult: avg: " + record.getField(i) + " and " + record.getField(recordSize - 1));
                resValues[j] = record.getDouble(i) / record.getDouble(recordSize - 1);
            } else {
                resValues[j] = record.getField(i);
            }
            j++;
            i++;
        }

        return new Record(resValues);
    }
}