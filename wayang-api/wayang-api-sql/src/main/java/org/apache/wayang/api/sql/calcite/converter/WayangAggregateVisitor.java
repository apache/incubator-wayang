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

package org.apache.wayang.api.sql.calcite.converter;

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.wayang.api.sql.calcite.rel.WayangAggregate;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.types.DataUnitType;

import java.util.*;

public class WayangAggregateVisitor extends WayangRelNodeVisitor<WayangAggregate> {

    WayangAggregateVisitor(WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(WayangAggregate wayangRelNode) {
        Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(0));
        Operator aggregateOperator;

        List<AggregateCall> aggregateCalls = ((Aggregate) wayangRelNode).getAggCallList();
        int groupCount = wayangRelNode.getGroupCount();
        Set<Integer> groupingFields = wayangRelNode.getGroupSet().asSet();

        MapOperator mapOperator = new MapOperator(
                new addAggCols(aggregateCalls),
                Record.class,
                Record.class
        );
        childOp.connectTo(0, mapOperator, 0);

        if(groupCount > 0){
            ReduceByOperator<Record, Object> reduceByOperator;
            reduceByOperator = new ReduceByOperator<>(
                    new TransformationDescriptor<>(new KeyExtractor(groupingFields), Record.class, Object.class),
                    new ReduceDescriptor<>(new aggregateFunction(aggregateCalls),
                            DataUnitType.createGrouped(Record.class),
                            DataUnitType.createBasicUnchecked(Record.class))
            );
            aggregateOperator = reduceByOperator;
        }
        else{
            GlobalReduceOperator<Record> globalReduceOperator;
            globalReduceOperator = new GlobalReduceOperator<>(
                    new ReduceDescriptor<>(new aggregateFunction(aggregateCalls),
                            DataUnitType.createGrouped(Record.class),
                            DataUnitType.createBasicUnchecked(Record.class))
            );
            aggregateOperator = globalReduceOperator;
        }

        mapOperator.connectTo(0, aggregateOperator, 0);

        MapOperator mapOperator2 = new MapOperator(
                new getResult(aggregateCalls, groupingFields),
                Record.class,
                Record.class
        );
        aggregateOperator.connectTo(0,mapOperator2,0);
        return mapOperator2;

    }
}

class KeyExtractor implements FunctionDescriptor.SerializableFunction<Record, Object> {
    private Set<Integer> indexSet;

    public KeyExtractor(Set<Integer> indexSet){
        this.indexSet = indexSet;
    }

    public Object apply(final Record record) {
        List<Object> keys = new ArrayList<>();
        for(Integer index : indexSet){
            keys.add(record.getField(index));
        }
        return keys;
    }
}

class addAggCols implements FunctionDescriptor.SerializableFunction<Record, Record> {
    private final List<AggregateCall> aggregateCalls;
    public addAggCols(List<AggregateCall> aggregateCalls)  {
        this.aggregateCalls = aggregateCalls;
    }
    @Override
    public Record apply(final Record record) {
        int l = record.size();
        int newRecordSize = l+aggregateCalls.size() +1;
        Object[] resValues = new Object[newRecordSize];
        int i;
        for(i=0; i<l; i++){
            resValues[i] = record.getField(i);
        }
        for(AggregateCall aggregateCall : aggregateCalls) {
            String name = aggregateCall.getAggregation().getName();
            if(name.equals("COUNT")){
                resValues[i] = 1;
            }
            else{
                resValues[i] = record.getField(aggregateCall.getArgList().get(0));
            }
            i++;
        }
        resValues[newRecordSize-1] = 1;
        return new Record(resValues);
    }
}

class getResult implements FunctionDescriptor.SerializableFunction<Record, Record> {
    private final List<AggregateCall> aggregateCallList;
    private Set<Integer> groupingfields;
    public getResult(List<AggregateCall> aggregateCalls, Set<Integer> groupingfields) {
        this.aggregateCallList = aggregateCalls;
        this.groupingfields = groupingfields;
    }

    @Override
    public Record apply(final Record record) {
        int l = record.size();
        int outputRecordSize = aggregateCallList.size() + groupingfields.size();
        Object[] resValues = new Object[outputRecordSize];

        int i = 0;
        int j = 0;
        for(i=0; j<groupingfields.size(); i++){
            if(groupingfields.contains(i)){
                resValues[j] = record.getField(i);
                j++;
            }
        }

        i = l - aggregateCallList.size() -1;
        for(AggregateCall aggregateCall : aggregateCallList){
            String name = aggregateCall.getAggregation().getName();
            if (name.equals("AVG")){
                resValues[j] = record.getDouble(i)/record.getDouble(l-1);
            }
            else{
                resValues[j] = record.getField(i);
            }
            j++;
            i++;
        }

        return new Record(resValues);
    }
}

class aggregateFunction implements FunctionDescriptor.SerializableBinaryOperator<Record> {
    private final List<AggregateCall> aggregateCallList;
    public aggregateFunction(List<AggregateCall> aggregateCalls) {
        this.aggregateCallList = aggregateCalls;
    }
    @Override
    public Record apply(Record record, Record record2) {
        int l = record.size();
        Object[] resValues = new Object[l];
        int i;
        boolean countDone = false;
        for(i=0; i<l-aggregateCallList.size() -1; i++){
            resValues[i] = record.getField(i);
        }
        for(AggregateCall aggregateCall : aggregateCallList) {
            String name = aggregateCall.getAggregation().getName();
            double val1 = record.getDouble(i);
            double val2 = record2.getDouble(i);
            if (name.equals("SUM")) {
                resValues[i] = val1 + val2;
            }
            else if (name.equals("MIN")) {
                resValues[i] = Math.min(val1, val2);
            }
            else if (name.equals("MAX")) {
                resValues[i] = Math.max(val1, val2);
            }
            else if (name.equals("COUNT")){
                resValues[i] = val1 + val2;
            }
            else if (name.equals("AVG")){
                resValues[i] = val1 + val2;
                if(!countDone) {
                    resValues[l-1] = record.getInt(l-1) + record2.getInt(l-1);
                    countDone = true;
                }
            }
            i++;
        }
        return new Record(resValues);
    }
}