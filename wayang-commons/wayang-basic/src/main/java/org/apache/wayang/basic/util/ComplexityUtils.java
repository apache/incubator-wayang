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

package org.apache.wayang.basic.util;

import org.apache.wayang.basic.operators.CoGroupOperator;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.FlatMapOperator;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.basic.operators.GroupByOperator;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.LoopOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.MapPartitionsOperator;
import org.apache.wayang.basic.operators.MaterializedGroupByOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.basic.operators.ReduceOperator;
import org.apache.wayang.basic.operators.SortOperator;
import org.apache.wayang.core.optimizer.ComplexityClass;
import org.apache.wayang.core.plan.wayangplan.Operator;

public class ComplexityUtils {
    /**
     * Infer complexity class from a given operator's descriptors.
     * @param operator
     * @return {@link ComplexityClass#LOGARITHMIC}, {@link ComplexityClass#LINEAR}, {@link ComplexityClass#QUADRATIC} or {@link ComplexityClass#SUPERQUADRATIC}. {@link ComplexityClass#LINEAR} on default
     */
    public static ComplexityClass inferFromOperator(final Operator operator) {
        if (operator instanceof final ReduceByOperator reduceBy) {
            return reduceBy.getReduceDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof final ReduceOperator reduce) {
            return reduce.getReduceDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof final GlobalReduceOperator globalReduce) {
            return globalReduce.getReduceDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof final CoGroupOperator coGroup) {
            return coGroup.getKeyDescriptor0().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof final GroupByOperator groupBy) {
            return groupBy.getKeyDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof final MaterializedGroupByOperator matGroupBy) {
            return matGroupBy.getKeyDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof final SortOperator sort) {
            return sort.getKeyDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof final JoinOperator join) {
            return join.getKeyDescriptor0().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof final MapOperator map) {
            return map.getFunctionDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof final FlatMapOperator flatMap) {
            return flatMap.getFunctionDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof final MapPartitionsOperator mapPartitions) {
            return mapPartitions.getFunctionDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof final FilterOperator filter) {
            return filter.getPredicateDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof final LoopOperator loop) {
            return loop.getCriterionDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        }

        return ComplexityClass.LINEAR;
    }
}
