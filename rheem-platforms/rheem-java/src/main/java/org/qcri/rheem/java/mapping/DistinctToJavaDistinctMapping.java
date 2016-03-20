package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.DistinctOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.operators.JavaDistinctOperator;
import org.qcri.rheem.java.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link DistinctOperator} to {@link JavaDistinctOperator}.
 */
public class DistinctToJavaDistinctMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory(),
                JavaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "distinct", new DistinctOperator<>(DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final DistinctOperator<?> originalOperator = (DistinctOperator<?>) subplanMatch.getMatch("distinct").getOperator();
            return new JavaDistinctOperator<>(originalOperator.getInputType()).at(epoch);
        }
    }
}
