package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.*;
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
public class SampleToSparkSampleMapping implements Mapping {

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
                "sample", new SampleOperator<>(0, DataSetType.none(), null), false
        ); //TODO: check if the zero here affects execution
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SampleOperator>(
                (matchedOperator, epoch) -> {
                    switch (matchedOperator.getSampleMethod()) {
                        case RANDOM:
                            return new SparkRandomPartitionSampleOperator<>(matchedOperator);
                        case SHUFFLE_FIRST:
                            return new SparkShufflePartitionSampleOperator<>(matchedOperator);
                        case BERNOULLI:
                            return new SparkBernoulliSampleOperator<>(matchedOperator);
                        default:
                            throw new RheemException(String.format(
                                    "%s sample method is not yet supported in Java platform.",
                                    matchedOperator.getSampleMethod()
                            ));
                            //FIXME: in case the user chose another Sample method but the optimizer chose to run in Java we get the exception. This should be fixed.
                    }
                }
        );
    }
}
