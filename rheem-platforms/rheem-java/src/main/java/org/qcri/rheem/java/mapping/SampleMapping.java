package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.OperatorPattern;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanPattern;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.operators.JavaRandomSampleOperator;
import org.qcri.rheem.java.operators.JavaReservoirSampleOperator;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link SampleOperator} to {@link JavaRandomSampleOperator}.
 */
@SuppressWarnings("unchecked")
public class SampleMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "sample", new SampleOperator<>(0, DataSetType.none(), null, 0L), false
        ).withAdditionalTest(op ->
                op.getSampleMethod() == SampleOperator.Methods.RANDOM
                        || op.getSampleMethod() == SampleOperator.Methods.RESERVOIR
                        || op.getSampleMethod() == SampleOperator.Methods.ANY
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SampleOperator>(
                (matchedOperator, epoch) -> {
                    switch (matchedOperator.getSampleMethod()) {
                        case ANY:
                        case RANDOM:
                            return new JavaRandomSampleOperator<>(matchedOperator).at(epoch);
                        case RESERVOIR:
                            return new JavaReservoirSampleOperator<>(matchedOperator).at(epoch);
                        default:
                            throw new RheemException(String.format(
                                    "%s sample method is not yet supported in Java platform.",
                                    matchedOperator.getSampleMethod()
                            ));
                    }
                }
        );
    }
}
