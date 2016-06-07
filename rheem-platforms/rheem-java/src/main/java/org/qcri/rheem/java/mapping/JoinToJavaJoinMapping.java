package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.operators.JavaJoinOperator;
import org.qcri.rheem.java.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link JoinOperator} to {@link JavaJoinOperator}.
 */
public class JoinToJavaJoinMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "join", new JoinOperator<>(null, null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<JoinOperator>(
                (matchedOperator, epoch) -> new JavaJoinOperator<>(
                        matchedOperator.getInputType0(),matchedOperator.getInputType1(),
                        matchedOperator.getKeyDescriptor0(),
                        matchedOperator.getKeyDescriptor1()
                ).at(epoch)
        );
    }
}
