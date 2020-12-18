package org.apache.incubator.wayang.spark.mapping;

import org.apache.incubator.wayang.basic.operators.SampleOperator;
import org.apache.incubator.wayang.core.api.exception.WayangException;
import org.apache.incubator.wayang.core.mapping.Mapping;
import org.apache.incubator.wayang.core.mapping.OperatorPattern;
import org.apache.incubator.wayang.core.mapping.PlanTransformation;
import org.apache.incubator.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.incubator.wayang.core.mapping.SubplanPattern;
import org.apache.incubator.wayang.core.types.DataSetType;
import org.apache.incubator.wayang.spark.operators.SparkBernoulliSampleOperator;
import org.apache.incubator.wayang.spark.operators.SparkRandomPartitionSampleOperator;
import org.apache.incubator.wayang.spark.operators.SparkShufflePartitionSampleOperator;
import org.apache.incubator.wayang.spark.platform.SparkPlatform;

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
                            throw new WayangException(String.format(
                                    "%s sample method is not yet supported in Sample platform.",
                                    matchedOperator.getSampleMethod()
                            ));
                    }
                }
        );
    }
}
