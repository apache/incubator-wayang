package org.qcri.rheem.basic.mapping;

import org.qcri.rheem.basic.operators.GroupByOperator;
import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.basic.operators.ReduceOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataSet;
import org.qcri.rheem.core.types.FlatDataSet;
import org.qcri.rheem.core.types.GroupedDataSet;

import java.util.Collection;
import java.util.Collections;

/**
 * This mapping detects combinations of the {@link GroupByOperator} and {@link ReduceOperator} and merges them into
 * a single {@link ReduceByOperator}.
 */
public class ReduceByMapping implements Mapping {


    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern groupByPattern = new OperatorPattern(
                "groupBy", new GroupByOperator<>(null, DataSet.flatAndBasic(Void.class),
                new GroupedDataSet(new BasicDataUnitType(Void.class))), false);
        final OperatorPattern reducePattern = new OperatorPattern(
                "reduce", new ReduceOperator<>(null, new GroupedDataSet(new BasicDataUnitType(Void.class)),
                DataSet.flatAndBasic(Void.class)), false);
        groupByPattern.connectTo(0, reducePattern, 0);
        return SubplanPattern.fromOperatorPatterns(groupByPattern, reducePattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch) {
            final GroupByOperator groupBy = (GroupByOperator) subplanMatch.getMatch("groupBy").getOperator();
            final ReduceOperator reduce = (ReduceOperator) subplanMatch.getMatch("reduce").getOperator();

            return new ReduceByOperator<>(
                    (FlatDataSet) groupBy.getInputType(),
                    groupBy.getKeyDescriptor(),
                    reduce.getReduceDescriptor());
        }
    }


}
