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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;

import org.apache.wayang.api.sql.calcite.converter.functions.AggregateAddCols;
import org.apache.wayang.api.sql.calcite.converter.functions.AggregateFunction;
import org.apache.wayang.api.sql.calcite.converter.functions.AggregateKeyExtractor;
import org.apache.wayang.api.sql.calcite.converter.functions.AggregateGetResult;
import org.apache.wayang.api.sql.calcite.rel.WayangAggregate;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.types.DataUnitType;

public class WayangAggregateVisitor extends WayangRelNodeVisitor<WayangAggregate> {

    WayangAggregateVisitor(final WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(final WayangAggregate wayangRelNode) {
        final Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(0));

        final List<AggregateCall> aggregateCalls = ((Aggregate) wayangRelNode).getAggCallList();
        final HashSet<Integer> groupingFields = new HashSet<>(wayangRelNode.getGroupSet().asSet());

        final MapOperator<Record, Record> mapOperator = new MapOperator<>(
                new AggregateAddCols(aggregateCalls),
                Record.class,
                Record.class);
        childOp.connectTo(0, mapOperator, 0);

        final Operator aggregateOperator = wayangRelNode.getGroupCount() > 0 ? new ReduceByOperator<>(
                new TransformationDescriptor<>(new AggregateKeyExtractor(groupingFields), Record.class, Object.class),
                new ReduceDescriptor<>(new AggregateFunction(aggregateCalls),
                        DataUnitType.createGrouped(Record.class),
                        DataUnitType.createBasicUnchecked(Record.class)))
                : new GlobalReduceOperator<>(
                        new ReduceDescriptor<>(new AggregateFunction(aggregateCalls),
                                DataUnitType.createGrouped(Record.class),
                                DataUnitType.createBasicUnchecked(Record.class)));

        mapOperator.connectTo(0, aggregateOperator, 0);

        final MapOperator<Record, Record> mapOperator2 = new MapOperator<>(
                new AggregateGetResult(aggregateCalls, groupingFields),
                Record.class,
                Record.class);
        aggregateOperator.connectTo(0, mapOperator2, 0);
        return mapOperator2;

    }
}