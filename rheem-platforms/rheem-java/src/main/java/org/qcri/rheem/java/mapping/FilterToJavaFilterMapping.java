package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.java.operators.JavaFilterOperator;
import org.qcri.rheem.java.plugin.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link FilterOperator} to {@link JavaFilterOperator}.
 */
public class FilterToJavaFilterMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory(),
                JavaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "filter", new FilterOperator<>(null, (PredicateDescriptor) null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final FilterOperator<?> originalOperator = (FilterOperator<?>) subplanMatch.getMatch("filter").getOperator();
            return new JavaFilterOperator(originalOperator.getInputType(),
                                            originalOperator.getPredicateDescriptor()).at(epoch);
        }
    }
}
