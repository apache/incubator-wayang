package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.RepeatOperator;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.OperatorPattern;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanPattern;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.operators.JavaRepeatOperator;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link RepeatOperator} to {@link JavaRepeatOperator}.
 */
@SuppressWarnings("unchecked")
public class RepeatMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "repeat", new RepeatOperator<>(1, DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }


    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<RepeatOperator>(
                (matchedOperator, epoch) -> new JavaRepeatOperator<>(matchedOperator).at(epoch)
        );
    }
}
