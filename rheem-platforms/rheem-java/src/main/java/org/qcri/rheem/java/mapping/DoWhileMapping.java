package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.DoWhileOperator;
import org.qcri.rheem.basic.operators.LoopOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.operators.JavaDoWhileOperator;
import org.qcri.rheem.java.operators.JavaLoopOperator;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link LoopOperator} to {@link JavaLoopOperator}.
 */
@SuppressWarnings("unchecked")
public class DoWhileMapping implements Mapping {

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
                "loop", new DoWhileOperator<>(null, null, (PredicateDescriptor)null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<DoWhileOperator>(
                (matchedOperator, epoch) -> new JavaDoWhileOperator<>(
                        matchedOperator.getInputType(),
                        matchedOperator.getConvergenceType(),
                        matchedOperator.getCriterionDescriptor()
                ).at(epoch)
        );
    }
}
