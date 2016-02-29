package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.CountOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.operators.JavaCountOperator;
import org.qcri.rheem.java.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link CountOperator} to {@link JavaCountOperator}.
 */
public class CountToJavaCountMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory(),
                JavaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "count", new CountOperator<>(DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final CountOperator<?> originalOperator = (CountOperator<?>) subplanMatch.getMatch("count").getOperator();
            return new JavaCountOperator<>(originalOperator.getInputType()).at(epoch);
        }
    }
}
