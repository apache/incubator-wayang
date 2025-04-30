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

import org.apache.calcite.rel.core.AggregateCall;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class AggregateAddCols implements FunctionDescriptor.SerializableFunction<Record, Record> {
    final List<AggregateCall> aggregateCalls;

    public AggregateAddCols(final List<AggregateCall> aggregateCalls) {
        this.aggregateCalls = aggregateCalls;
    }

    @Override
    public Record apply(final Record record) {
        final int l = record.size();
        final int newRecordSize = l + aggregateCalls.size() + 1;
        final Object[] resValues = new Object[newRecordSize];

        for (int i = 0; i < l; i++) {
            resValues[i] = record.getField(i);
        }

        int i = l;
        for (final AggregateCall aggregateCall : aggregateCalls) {
            switch (aggregateCall.getAggregation().kind) {
                case COUNT:
                    resValues[i] = 1;
                    break;
                default:
                    resValues[i] = record.getField(aggregateCall.getArgList().get(0));
            }
            i++;
        }

        resValues[newRecordSize - 1] = 1;
        System.out.println("AddCols: returning res valueS: " + Arrays.toString(resValues) + ", vs rec: " + record);
        return new Record(resValues);
    }
}