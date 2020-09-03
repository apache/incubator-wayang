package io.rheem.rheem.spark.mapping;

import io.rheem.rheem.basic.operators.FilterOperator;
import io.rheem.rheem.core.function.PredicateDescriptor;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.spark.operators.SparkFilterOperator;
import io.rheem.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link FilterOperator} to {@link SparkFilterOperator}.
 */
@SuppressWarnings("unchecked")
public class FilterMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                SparkPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "filter", new FilterOperator<>((PredicateDescriptor) null, DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<FilterOperator>(
                (matchedOperator, epoch) -> new SparkFilterOperator<>(matchedOperator).at(epoch)
        );
    }
}
