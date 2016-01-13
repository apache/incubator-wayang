package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.CollocateByOperator;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.java.operators.JavaCollocateByOperator;
import org.qcri.rheem.java.operators.JavaTextFileSource;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link TextFileSource} to {@link JavaTextFileSource}.
 */
public class JavaCollocateByOperatorMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "operator", new CollocateByOperator<>(null, null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch) {
            final CollocateByOperator<?, ?> originalOperator = (CollocateByOperator<?, ?>) subplanMatch.getMatch("operator").getOperator();
            return new JavaCollocateByOperator<>(
                    originalOperator.getType().unchecked(),
                    originalOperator.getKeyDescriptor().unchecked()
            );
        }
    }
}
