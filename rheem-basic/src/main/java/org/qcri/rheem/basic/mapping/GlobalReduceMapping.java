package org.qcri.rheem.basic.mapping;

import org.qcri.rheem.basic.operators.GlobalReduceOperator;
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
public class GlobalReduceMapping implements Mapping {


    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern reducePattern = new OperatorPattern(
                "reduce", new ReduceOperator<>(null, new GroupedDataSet(new BasicDataUnitType(Void.class)),
                DataSet.flatAndBasic(Void.class)), false);
        return SubplanPattern.createSingleton(reducePattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch) {
            final ReduceOperator reduce = (ReduceOperator) subplanMatch.getMatch("reduce").getOperator();

            return new GlobalReduceOperator<>(
                    (FlatDataSet) reduce.getInputType(),
                    reduce.getReduceDescriptor());
        }
    }


}
