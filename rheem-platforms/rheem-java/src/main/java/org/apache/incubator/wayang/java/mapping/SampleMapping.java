package io.rheem.rheem.java.mapping;

import io.rheem.rheem.basic.operators.SampleOperator;
import io.rheem.rheem.core.api.exception.RheemException;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.java.operators.JavaRandomSampleOperator;
import io.rheem.rheem.java.operators.JavaReservoirSampleOperator;
import io.rheem.rheem.java.platform.JavaPlatform;

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
