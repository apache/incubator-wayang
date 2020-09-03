package io.rheem.rheem.spark.mapping;

import io.rheem.rheem.basic.operators.FlatMapOperator;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.spark.operators.SparkFlatMapOperator;
import io.rheem.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link FlatMapOperator} to {@link SparkFlatMapOperator}.
 */
@SuppressWarnings("unchecked")
public class FlatMapMapping implements Mapping {

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
                "flatMap", new FlatMapOperator<>(null, DataSetType.none(), DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<FlatMapOperator>(
                (matchedOperator, epoch) -> new SparkFlatMapOperator<>(matchedOperator).at(epoch)
        );
    }
}
