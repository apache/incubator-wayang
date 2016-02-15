package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.CartesianOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.java.operators.JavaCartesianOperator;
import org.qcri.rheem.java.plugin.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link CartesianOperator} to {@link JavaCartesianOperator}.
 */
public class CartesianToJavaCartesianMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "cartesian", new CartesianOperator<>(null, null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<JavaCartesianOperator>(
                (matchedOperator, epoch) -> new JavaCartesianOperator<>(
                        matchedOperator.getInputType0(),
                        matchedOperator.getInputType1()
                ).at(epoch)
        );
    }
}
