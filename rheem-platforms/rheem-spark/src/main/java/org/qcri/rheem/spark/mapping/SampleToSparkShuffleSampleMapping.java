package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.spark.operators.SparkShufflePartitionSampleOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link SampleOperator} to {@link SparkShufflePartitionSampleOperator}.
 */
@SuppressWarnings("unchecked")
public class SampleToSparkShuffleSampleMapping implements Mapping {

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
                "shuffle_partition", new SampleOperator<>((long) 0, (PredicateDescriptor) null, null), false); //TODO: check if the zero here affects execution
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SampleOperator>(
                (matchedOperator, epoch) -> new SparkShufflePartitionSampleOperator<>(
                        matchedOperator.getSampleSize(),
                        matchedOperator.getType(),
                        matchedOperator.getPredicateDescriptor()
                ).at(epoch)
        );
    }
}
