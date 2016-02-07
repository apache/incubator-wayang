package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.spark.operators.SparkFlatMapOperator;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link FlatMapOperator} to {@link SparkFlatMapOperator}.
 */
public class FlatMapToSparkFlatMapMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "flatMap", new FlatMapOperator<>(null, null, null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final FlatMapOperator<?, ?> originalOperator = (FlatMapOperator<?, ?>) subplanMatch.getMatch("flatMap").getOperator();
            return new SparkFlatMapOperator(originalOperator.getInputType(),
                    originalOperator.getOutputType(),
                    originalOperator.getFunctionDescriptor()).at(epoch);
        }
    }
}
