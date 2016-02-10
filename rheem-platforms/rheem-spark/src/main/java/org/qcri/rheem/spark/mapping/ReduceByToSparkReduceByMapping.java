package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.spark.operators.SparkReduceByOperator;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link ReduceByOperator} to {@link SparkReduceByOperator}.
 */
public class ReduceByToSparkReduceByMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "reduceBy", new ReduceByOperator<>(null, null, null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final ReduceByOperator<?, ?> originalOperator = (ReduceByOperator<?, ?>) subplanMatch.getMatch("reduceBy").getOperator();
            return new SparkReduceByOperator<>(
                    originalOperator.getType().unchecked(),
                    originalOperator.getKeyDescriptor().unchecked(),
                    originalOperator.getReduceDescriptor().unchecked()
            ).at(epoch);
        }
    }
}
