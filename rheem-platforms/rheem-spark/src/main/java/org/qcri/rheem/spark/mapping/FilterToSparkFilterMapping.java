package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.spark.operators.SparkFilterOperator;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link FilterOperator} to {@link SparkFilterOperator}.
 */
public class FilterToSparkFilterMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "filter", new FilterOperator<>(null, null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final FilterOperator<?> originalOperator = (FilterOperator<?>) subplanMatch.getMatch("filter").getOperator();
            return new SparkFilterOperator(originalOperator.getInputType(),
                                            originalOperator.getFunctionDescriptor()).at(epoch);
        }
    }
}
