package org.apache.wayang.ml.util;

import org.apache.wayang.core.function.UDFComplexity;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.basic.operators.*;

public class Udf {

    public static UDFComplexity getComplexity(ExecutionOperator operator) {
        if (operator instanceof ReduceByOperator) {
            return ((ReduceByOperator) operator).getReduceDescriptor().getUDFComplexity();
        }

        if (operator instanceof ReduceOperator) {
            return ((ReduceOperator) operator).getReduceDescriptor().getUDFComplexity();
        }

        if (operator instanceof CoGroupOperator) {
            return ((CoGroupOperator) operator).getKeyDescriptor0().getUDFComplexity();
        }

        if (operator instanceof FilterOperator) {
            return ((FilterOperator) operator).getPredicateDescriptor().getUDFComplexity();
        }

        if (operator instanceof FlatMapOperator) {
            return ((FlatMapOperator) operator).getFunctionDescriptor().getUDFComplexity();
        }

        if (operator instanceof GlobalReduceOperator) {
            return ((GlobalReduceOperator) operator).getReduceDescriptor().getUDFComplexity();
        }
        if (operator instanceof GroupByOperator) {
            return ((GroupByOperator) operator).getKeyDescriptor().getUDFComplexity();
        }

        if (operator instanceof JoinOperator) {
            return ((JoinOperator) operator).getKeyDescriptor0().getUDFComplexity();
        }

        if (operator instanceof LoopOperator) {
            return ((LoopOperator) operator).getCriterionDescriptor().getUDFComplexity();
        }

        if (operator instanceof MapOperator) {
            return ((MapOperator) operator).getFunctionDescriptor().getUDFComplexity();
        }

        if (operator instanceof MapPartitionsOperator) {
            return ((MapPartitionsOperator) operator).getFunctionDescriptor().getUDFComplexity();
        }

        if (operator instanceof MaterializedGroupByOperator) {
            return ((MaterializedGroupByOperator) operator).getKeyDescriptor().getUDFComplexity();
        }

        if (operator instanceof SortOperator) {
            return ((SortOperator) operator).getKeyDescriptor().getUDFComplexity();
        }

        return UDFComplexity.LINEAR;
    }
}
