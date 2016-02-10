package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.java.operators.JavaReduceByOperator;
import org.qcri.rheem.java.plugin.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link ReduceByOperator} to {@link JavaReduceByOperator}.
 */
public class ReduceByOperatorToJavaReduceByOperatorMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(createSubplanPattern(), new ReplacementFactory(),
                JavaPlatform.getInstance()));
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
            return new JavaReduceByOperator<>(
                    originalOperator.getType().unchecked(),
                    originalOperator.getKeyDescriptor().unchecked(),
                    originalOperator.getReduceDescriptor().unchecked()
            ).at(epoch);
        }
    }
}
