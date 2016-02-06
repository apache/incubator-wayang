package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.CoalesceOperator;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.java.operators.JavaCoalesceOperator;
import org.qcri.rheem.java.operators.JavaMapOperator;
import org.qcri.rheem.java.operators.JavaTextFileSource;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link TextFileSource} to {@link JavaTextFileSource}.
 */
public class JavaCoalesceOperatorMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "coalesce", new CoalesceOperator<>(null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final CoalesceOperator<?> originalOperator = (CoalesceOperator<?>) subplanMatch.getMatch("coalesce").getOperator();
            return new JavaCoalesceOperator<>(originalOperator.getOutput().getType()).at(epoch);
        }
    }
}
