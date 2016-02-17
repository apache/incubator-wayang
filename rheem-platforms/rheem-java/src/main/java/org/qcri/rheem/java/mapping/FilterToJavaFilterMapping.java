package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.java.operators.JavaFilterOperator;
import org.qcri.rheem.java.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link FilterOperator} to {@link JavaFilterOperator}.
 */
@SuppressWarnings("unchecked")
public class FilterToJavaFilterMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        JavaPlatform.getInstance()
                )
        );
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "filter", new FilterOperator<>(null, (PredicateDescriptor) null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<FilterOperator>(
                (matchedOperator, epoch) -> new JavaFilterOperator<>(
                        matchedOperator.getType(),
                        matchedOperator.getPredicateDescriptor()
                ).at(epoch)
        );
    }
}
