package io.rheem.rheem.spark.mapping;

import io.rheem.rheem.basic.operators.RepeatOperator;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.spark.operators.SparkRepeatOperator;
import io.rheem.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link RepeatOperator} to {@link SparkRepeatOperator}.
 */
@SuppressWarnings("unchecked")
public class RepeatMapping implements Mapping {

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
                "repeat", new RepeatOperator<>(1, DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }


    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<RepeatOperator>(
                (matchedOperator, epoch) -> new SparkRepeatOperator<>(matchedOperator).at(epoch)
        );
    }
}
