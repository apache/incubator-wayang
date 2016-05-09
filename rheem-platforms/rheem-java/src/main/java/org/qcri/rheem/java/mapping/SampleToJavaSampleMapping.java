package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.operators.JavaRandomSampleOperator;
import org.qcri.rheem.java.operators.JavaReservoirSampleOperator;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.logging.Logger;

/**
 * Mapping from {@link SampleOperator} to {@link JavaRandomSampleOperator}.
 */
@SuppressWarnings("unchecked")
public class SampleToJavaSampleMapping implements Mapping {

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
                "sample", new SampleOperator<>(0, null, null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SampleOperator>(
                (matchedOperator, epoch) -> {
                    switch (matchedOperator.getSampleMethod()) {
                        case RANDOM:
                            if (matchedOperator.getDatasetSize() > 0)
                                return new JavaRandomSampleOperator<>(
                                        matchedOperator.getSampleSize(),
                                        matchedOperator.getDatasetSize(),
                                        matchedOperator.getType()).at(epoch);
                            else
                                return new JavaRandomSampleOperator<>(
                                        matchedOperator.getSampleSize(),
                                        matchedOperator.getType()).at(epoch);
                        case RESERVOIR:
                            if (matchedOperator.getDatasetSize() > 0)
                                return new JavaReservoirSampleOperator<>(
                                        matchedOperator.getSampleSize(),
                                        matchedOperator.getDatasetSize(),
                                        matchedOperator.getType()).at(epoch);
                            else
                                return new JavaReservoirSampleOperator<>(
                                        matchedOperator.getSampleSize(),
                                        matchedOperator.getType()).at(epoch);
                        default:
                            throw new RheemException(String.format("%s sample method is not yet supported in Java platform.", matchedOperator.getSampleMethod().toString()));
                    }
                }
        );
    }
}
