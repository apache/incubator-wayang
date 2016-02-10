package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.CountOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.spark.operators.SparkCountOperator;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link CountOperator} to {@link SparkCountOperator}.
 */
public class CountToSparkCountMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "count", new CountOperator<>(null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final CountOperator<?> originalOperator = (CountOperator<?>) subplanMatch.getMatch("count").getOperator();
            return new SparkCountOperator<>(originalOperator.getInputType()).at(epoch);
        }
    }
}
