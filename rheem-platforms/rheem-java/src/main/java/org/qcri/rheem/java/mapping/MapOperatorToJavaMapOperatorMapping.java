package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.java.operators.JavaMapOperator;
import org.qcri.rheem.java.operators.JavaTextFileSource;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link TextFileSource} to {@link JavaTextFileSource}.
 */
public class MapOperatorToJavaMapOperatorMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "map", new MapOperator<>(null, null, null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final MapOperator<?, ?> originalOperator = (MapOperator<?, ?>) subplanMatch.getMatch("map").getOperator();
            return new JavaMapOperator(originalOperator.getInputType(),
                    originalOperator.getOutputType(),
                    originalOperator.getFunctionDescriptor()).at(epoch);
        }
    }
}
