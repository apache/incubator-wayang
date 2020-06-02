package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.OperatorPattern;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanPattern;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.operators.SparkBernoulliSampleOperator;
import org.qcri.rheem.spark.operators.SparkRandomPartitionSampleOperator;
import org.qcri.rheem.spark.operators.SparkShufflePartitionSampleOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

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
