package io.rheem.rheem.spark.mapping;

import io.rheem.rheem.basic.operators.SampleOperator;
import io.rheem.rheem.core.api.exception.RheemException;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.spark.operators.SparkBernoulliSampleOperator;
import io.rheem.rheem.spark.operators.SparkRandomPartitionSampleOperator;
import io.rheem.rheem.spark.operators.SparkShufflePartitionSampleOperator;
import io.rheem.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link SampleOperator} to {@link SparkRandomPartitionSampleOperator}.
 */
@SuppressWarnings("unchecked")
public class SampleMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                SparkPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "sample", new SampleOperator<>(0, DataSetType.none(), null, 0L), false
        ).withAdditionalTest(op ->
                op.getSampleMethod() == SampleOperator.Methods.RANDOM
                        || op.getSampleMethod() == SampleOperator.Methods.SHUFFLE_PARTITION_FIRST
                        || op.getSampleMethod() == SampleOperator.Methods.BERNOULLI
                        || op.getSampleMethod() == SampleOperator.Methods.ANY
        ); //TODO: check if the zero here affects execution
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SampleOperator>(
                (matchedOperator, epoch) -> {
                    switch (matchedOperator.getSampleMethod()) {
                        case RANDOM:
                            return new SparkRandomPartitionSampleOperator<>(matchedOperator);
                        case ANY:
                        case SHUFFLE_PARTITION_FIRST:
                            return new SparkShufflePartitionSampleOperator<>(matchedOperator);
                        case BERNOULLI:
                            return new SparkBernoulliSampleOperator<>(matchedOperator);
                        default:
                            throw new RheemException(String.format(
                                    "%s sample method is not yet supported in Sample platform.",
                                    matchedOperator.getSampleMethod()
                            ));
                    }
                }
        );
    }
}
