package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.operators.SparkBernoulliSampleOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link SampleOperator} to {@link SparkBernoulliSampleOperator}.
 */
@SuppressWarnings("unchecked")
public class SampleToSparkBernoulliSampleMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        SparkPlatform.getInstance()
                )
        );
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "bernoulli_sample", new SampleOperator<>(0, 0, null), false); //TODO: check if the zero here affects execution
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SampleOperator>(
                (matchedOperator, epoch) -> {
                    if (matchedOperator.getDatasetSize() > 0)
                        return new SparkBernoulliSampleOperator<>(
                                matchedOperator.getSampleSize(),
                                matchedOperator.getDatasetSize(),
                                matchedOperator.getType()
                        ).at(epoch);
                    else
                        return new SparkBernoulliSampleOperator<>(
                                matchedOperator.getSampleSize(),
                                matchedOperator.getType()
                        ).at(epoch);
                }
        );
    }
}
