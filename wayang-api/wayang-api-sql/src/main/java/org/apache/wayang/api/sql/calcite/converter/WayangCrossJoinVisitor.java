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

import java.io.Serializable;

import org.apache.wayang.api.sql.calcite.converter.functions.JoinFlattenResult;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.CartesianOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.util.ReflectionUtils;

public class WayangCrossJoinVisitor extends WayangRelNodeVisitor<WayangJoin> implements Serializable {

    WayangCrossJoinVisitor(final WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(final WayangJoin wayangRelNode) {
        final Operator childOpLeft = wayangRelConverter.convert(wayangRelNode.getInput(0));
        final Operator childOpRight = wayangRelConverter.convert(wayangRelNode.getInput(1));

        final CartesianOperator<Record, Record> join = new CartesianOperator<Record, Record>(
                Record.class,
                Record.class);

        childOpLeft.connectTo(0, join, 0);
        childOpRight.connectTo(0, join, 1);

        final SerializableFunction<Tuple2<Record, Record>, Record> mp = new JoinFlattenResult();

        final MapOperator<Tuple2<Record, Record>, Record> mapOperator = new MapOperator<Tuple2<Record, Record>, Record>(
                mp,
                ReflectionUtils.specify(Tuple2.class),
                Record.class);

        join.connectTo(0, mapOperator, 0);

        return mapOperator;
    }

}
