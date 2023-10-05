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
import org.apache.wayang.core.util.Tuple;

import java.util.List;

public class WayangAggregateVisitor extends WayangRelNodeVisitor<WayangAggregate> {

    WayangAggregateVisitor(WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(WayangAggregate wayangRelNode) {
        Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(0));

        List<AggregateCall> aggregateCalls = ((Aggregate) wayangRelNode).getAggCallList();

        for (AggregateCall aggregateCall : aggregateCalls) {
            if (aggregateCall.getAggregation().getName().equals("SUM")) {
                int fieldIndex = aggregateCall.getArgList().get(0);
                //System.out.println(fieldIndex);
                int groupCount = wayangRelNode.getGroupCount();
                //System.out.println(groupCount);
                if (groupCount > 0) {
                    int groupIndex = 0; // wayangRelNode.getGroupSets()
                    // Create the ReduceByOperator
                    ReduceByOperator<Record, Object> reduceByOperator;
                    reduceByOperator = new ReduceByOperator<>(
                            new TransformationDescriptor<>(new KeyExtractor(groupIndex), Record.class, Object.class),
                            new ReduceDescriptor<>(new SumFunction(fieldIndex),
                                    DataUnitType.createGrouped(Record.class),
                                    DataUnitType.createBasicUnchecked(Record.class))
                    );
                    // Connect it to the child operator
                    childOp.connectTo(0, reduceByOperator, 0);
                    return reduceByOperator;
                }
                else {
                    GlobalReduceOperator<Record> globalReduceOperator;
                    globalReduceOperator = new GlobalReduceOperator<>(
                            new ReduceDescriptor<>(new SumFunction(fieldIndex),
                                    DataUnitType.createGrouped(Record.class),
                                    DataUnitType.createBasicUnchecked(Record.class))
                    );
                    childOp.connectTo(0,globalReduceOperator,0);
                    return globalReduceOperator;
                }
            }
            else if (aggregateCall.getAggregation().getName().equals("MIN")) {
                int fieldIndex = aggregateCall.getArgList().get(0);
                //System.out.println(fieldIndex);
                int groupCount = wayangRelNode.getGroupCount();
                //System.out.println(groupCount);
                if (groupCount > 0) {
                    int groupIndex = 0; // wayangRelNode.getGroupSets()
                    // Create the ReduceByOperator
                    ReduceByOperator<Record, Object> reduceByOperator;
                    reduceByOperator = new ReduceByOperator<>(
                            new TransformationDescriptor<>(new KeyExtractor(groupIndex), Record.class, Object.class),
                            new ReduceDescriptor<>(new minFunction(fieldIndex),
                                    DataUnitType.createGrouped(Record.class),
                                    DataUnitType.createBasicUnchecked(Record.class))
                    );
                    // Connect it to the child operator
                    childOp.connectTo(0, reduceByOperator, 0);
                    return reduceByOperator;
                }
                else {
                    GlobalReduceOperator<Record> globalReduceOperator;
                    globalReduceOperator = new GlobalReduceOperator<>(
                            new ReduceDescriptor<>(new minFunction(fieldIndex),
                                    DataUnitType.createGrouped(Record.class),
                                    DataUnitType.createBasicUnchecked(Record.class))
                    );
                    childOp.connectTo(0,globalReduceOperator,0);
                    return globalReduceOperator;
                }
            }
            else if (aggregateCall.getAggregation().getName().equals("MAX")) {
                int fieldIndex = aggregateCall.getArgList().get(0);
                //System.out.println(fieldIndex);
                int groupCount = wayangRelNode.getGroupCount();
                //System.out.println(groupCount);
                if (groupCount > 0) {
                    int groupIndex = 0; // wayangRelNode.getGroupSets()
                    // Create the ReduceByOperator
                    ReduceByOperator<Record, Object> reduceByOperator;
                    reduceByOperator = new ReduceByOperator<>(
                            new TransformationDescriptor<>(new KeyExtractor(groupIndex), Record.class, Object.class),
                            new ReduceDescriptor<>(new maxFunction(fieldIndex),
                                    DataUnitType.createGrouped(Record.class),
                                    DataUnitType.createBasicUnchecked(Record.class))
                    );
                    // Connect it to the child operator
                    childOp.connectTo(0, reduceByOperator, 0);
                    return reduceByOperator;
                }
                else {
                    GlobalReduceOperator<Record> globalReduceOperator;
                    globalReduceOperator = new GlobalReduceOperator<>(
                            new ReduceDescriptor<>(new maxFunction(fieldIndex),
                                    DataUnitType.createGrouped(Record.class),
                                    DataUnitType.createBasicUnchecked(Record.class))
                    );
                    childOp.connectTo(0,globalReduceOperator,0);
                    return globalReduceOperator;
                }
            }
            else if (aggregateCall.getAggregation().getName().equals("AVG")) {
                //System.out.println(aggregateCall.getArgList());
                int fieldIndex = aggregateCall.getArgList().get(0);
                //System.out.println(fieldIndex);
                MapOperator mapOperator1 = new MapOperator(
                        new addCountCol(),
                        Record.class,
                        Record.class
                );
                childOp.connectTo(0, mapOperator1, 0);

                int groupCount = wayangRelNode.getGroupCount();
                //System.out.println(groupCount);
                if (groupCount > 0) {
                    int groupIndex = 0; // wayangRelNode.getGroupSets()
                    // Create the ReduceByOperator
                    ReduceByOperator<Record, Object> reduceByOperator;
                    reduceByOperator = new ReduceByOperator<>(
                            new TransformationDescriptor<>(new KeyExtractor(groupIndex), Record.class, Object.class),
                            new ReduceDescriptor<>(new avgFunction(fieldIndex),
                                    DataUnitType.createGrouped(Record.class),
                                    DataUnitType.createBasicUnchecked(Record.class))
                    );
                    // Connect it to the child operator
                    mapOperator1.connectTo(0, reduceByOperator, 0);
                    MapOperator mapOperator2 = new MapOperator(
                            new getAvg(fieldIndex),
                            Record.class,
                            Record.class
                    );
                    reduceByOperator.connectTo(0,mapOperator2,0);
                    return mapOperator2;
                }
                else {
                    GlobalReduceOperator<Record> globalReduceOperator;
                    globalReduceOperator = new GlobalReduceOperator<>(
                            new ReduceDescriptor<>(new avgFunction(fieldIndex),
                                    DataUnitType.createGrouped(Record.class),
                                    DataUnitType.createBasicUnchecked(Record.class))
                    );
                    mapOperator1.connectTo(0,globalReduceOperator,0);
                    MapOperator mapOperator2 = new MapOperator(
                            new getAvg(fieldIndex),
                            Record.class,
                            Record.class
                    );
                    globalReduceOperator.connectTo(0,mapOperator2,0);
                    return mapOperator2;
                }

            } else {
                throw new UnsupportedOperationException("Unsupported aggregate function: " +
                        aggregateCall.getAggregation().getName());
            }
        }

        return childOp;
    }
}
class KeyExtractor implements FunctionDescriptor.SerializableFunction<Record, Object> {
    private final int index;

    public KeyExtractor(int index) {
        this.index = index;
    }

    public Object apply(final Record record) {
        return record.getField(index);
    }
}

class KeyExtractorTuple implements FunctionDescriptor.SerializableFunction<Tuple<Record,Integer>, Object> {
    private final int index;

    public KeyExtractorTuple(int index) {
        this.index = index;
    }

    public Object apply(final Tuple<Record,Integer> tuple) {
        return tuple.field0.getField(index);
    }
}

class avgFunction implements FunctionDescriptor.SerializableBinaryOperator<Record> {
    private final int fieldIndex;
    public avgFunction(int fieldIndex) {
        this.fieldIndex = fieldIndex;
    }
    @Override
    public Record apply(Record record, Record record2) {
        double sum = 0;
        sum = record.getDouble(fieldIndex) + record2.getDouble(fieldIndex);
        int l = record.size();
        int totalCount =  record.getInt(l-1) + record2.getInt(l-1);
        Object[] resValues = new Object[l];
        for(int i=0; i<l-1; i++){
            if(i==fieldIndex){
                resValues[i] = sum;
            }
            else{
                resValues[i] = record.getField(i);
            }
        }
        resValues[l-1] = totalCount;
        return new Record(resValues);
    }
}
class countFunction implements FunctionDescriptor.SerializableBinaryOperator<Record> {
    public countFunction() {}
    @Override
    public Record apply(Record record, Record record2) {
        int l = record.size();
        int totalCount =  record.getInt(l-1) + record2.getInt(l-1);
        Object[] resValues = new Object[l];
        for(int i=0; i<l-1; i++){
            resValues[i] = record.getField(i);
        }
        resValues[l-1] = totalCount;
        return new Record(resValues);
    }
}
class minFunction implements FunctionDescriptor.SerializableBinaryOperator<Record> {
    private final int fieldIndex;
    public minFunction(int fieldIndex) {
        this.fieldIndex = fieldIndex;
    }
    @Override
    public Record apply(Record record, Record record2) {
        if (((Number) record.getField(fieldIndex)).doubleValue() < ((Number) record2.getField(fieldIndex)).doubleValue()) {
            return record;
        } else {
            return record2;
        }
    }
}

class maxFunction implements FunctionDescriptor.SerializableBinaryOperator<Record> {
    private final int fieldIndex;
    public maxFunction(int fieldIndex) {
        this.fieldIndex = fieldIndex;
    }
    @Override
    public Record apply(Record record, Record record2) {
        if (((Number) record.getField(fieldIndex)).doubleValue() > ((Number) record2.getField(fieldIndex)).doubleValue()) {
            return record;
        } else {
            return record2;
        }
    }
}

class SumFunction implements FunctionDescriptor.SerializableBinaryOperator<Record> {
    private final int fieldIndex;
    public SumFunction(int fieldIndex) {
        this.fieldIndex = fieldIndex;
    }
    @Override
    public Record apply(Record record, Record record2) {
        double sum = 0;
        sum = record.getDouble(fieldIndex) + record2.getDouble(fieldIndex);
        //create an array storing values of resrecord
        int l = record.size();
        Object[] resValues = new Object[l];
        for(int i=0; i<l; i++){
            if(i==fieldIndex){
                resValues[i] = sum;
            }
            else{
                resValues[i] = record.getField(i);
            }
        }
        return new Record(resValues);
    }
}

class addCountCol implements FunctionDescriptor.SerializableFunction<Record, Record> {
    public addCountCol() {}
    @Override
    public Record apply(final Record record) {
        int l = record.size();
        int count = 1;
        Object[] resValues = new Object[l+1];
        for(int i=0; i<l; i++){
            resValues[i] = record.getField(i);
        }
        resValues[l] = count;
        return new Record(resValues);
    }
}

class removeCountCol implements FunctionDescriptor.SerializableFunction<Record, Record> {
    public removeCountCol() {}

    @Override
    public Record apply(final Record record) {
        int l = record.size();
        int count = record.getInt(l-1);
        Object[] resValues = new Object[l-1];
        resValues[0] = count;
        for(int i=0; i<l-2; i++){
            resValues[i+1] = record.getField(i);
        }

        return new Record(resValues);

    }
}

class getAvg implements FunctionDescriptor.SerializableFunction<Record, Record> {
    private final int fieldIndex;
    public getAvg(int fieldindex) {
        this.fieldIndex = fieldindex;
    }

    @Override
    public Record apply(final Record record) {
        int l = record.size();
        int count = record.getInt(l-1);
        double sum = record.getDouble(fieldIndex);
        double avg = sum/count;
        Object[] resValues = new Object[l-1];
        for(int i=0; i<l-1; i++){
            if(i==fieldIndex){
                resValues[i] = avg;
            }
            else{
                resValues[i] = record.getField(i);
            }
        }
        return new Record(resValues);

    }
}
