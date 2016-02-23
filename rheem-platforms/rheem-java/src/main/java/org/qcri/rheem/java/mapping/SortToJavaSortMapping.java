package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.SortOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.operators.JavaSortOperator;
import org.qcri.rheem.java.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link SortOperator} to {@link JavaSortOperator}.
 */
public class SortToJavaSortMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory(),
                JavaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "sort", new SortOperator<>(DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final SortOperator<?> originalOperator = (SortOperator<?>) subplanMatch.getMatch("sort").getOperator();
            return new JavaSortOperator<>(originalOperator.getInputType()).at(epoch);
        }
    }
}
