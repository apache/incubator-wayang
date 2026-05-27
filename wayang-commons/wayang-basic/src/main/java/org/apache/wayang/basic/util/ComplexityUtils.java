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
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.profiling.ComplexityClass;

public class ComplexityUtils {
    /**
     * Infer complexity class from a given operator's descriptors.
     * @param operator
     * @return {@link ComplexityClass#LOGARITHMIC}, {@link ComplexityClass#LINEAR}, {@link ComplexityClass#QUADRATIC} or {@link ComplexityClass#SUPERQUADRATIC}. {@link ComplexityClass#LINEAR} on default
     */
    public static ComplexityClass inferFromOperator(Operator operator) {
        if (operator instanceof ReduceByOperator reduceBy) {
            return reduceBy.getReduceDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof ReduceOperator reduce) {
            return reduce.getReduceDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof GlobalReduceOperator globalReduce) {
            return globalReduce.getReduceDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof CoGroupOperator coGroup) {
            return coGroup.getKeyDescriptor0().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof GroupByOperator groupBy) {
            return groupBy.getKeyDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof MaterializedGroupByOperator matGroupBy) {
            return matGroupBy.getKeyDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof SortOperator sort) {
            return sort.getKeyDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof JoinOperator join) {
            return join.getKeyDescriptor0().getComplexityClass().orElse(ComplexityClass.LINEAR);re
        } else if (operator instanceof MapOperator map) {
            return map.getFunctionDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof FlatMapOperator flatMap) {
            return flatMap.getFunctionDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof MapPartitionsOperator mapPartitions) {
            return mapPartitions.getFunctionDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof FilterOperator filter) {
            return filter.getPredicateDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        } else if (operator instanceof LoopOperator loop) {
            return loop.getCriterionDescriptor().getComplexityClass().orElse(ComplexityClass.LINEAR);
        }

        return ComplexityClass.LINEAR;
    }
}
