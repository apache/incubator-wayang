package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.java.operators.JavaFlatMapOperator;
import org.qcri.rheem.java.plugin.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link FlatMapOperator} to {@link JavaFlatMapOperator}.
 */
public class FlatMapToJavaFlatMapMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(createSubplanPattern(), new ReplacementFactory(),
                JavaPlatform.getInstance()));
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
            return new JavaFlatMapOperator(originalOperator.getInputType(),
                    originalOperator.getOutputType(),
                    originalOperator.getFunctionDescriptor()).at(epoch);
        }
    }
}
