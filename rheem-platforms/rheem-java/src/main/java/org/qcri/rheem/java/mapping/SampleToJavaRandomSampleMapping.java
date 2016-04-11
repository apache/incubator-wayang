package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.operators.JavaRandomSampleOperator;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link SampleOperator} to {@link JavaRandomSampleOperator}.
 */
@SuppressWarnings("unchecked")
public class SampleToJavaRandomSampleMapping implements Mapping {

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
                "random_sample", new SampleOperator<>(0, (DataSetType) null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SampleOperator>(
                (matchedOperator, epoch) -> new JavaRandomSampleOperator<>(
                        matchedOperator.getSampleSize(),
                        matchedOperator.getPredicateDescriptor()
                ).at(epoch)
        );
    }
}
