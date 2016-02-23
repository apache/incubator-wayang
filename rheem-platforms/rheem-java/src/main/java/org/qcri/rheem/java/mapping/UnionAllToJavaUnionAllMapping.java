package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.UnionAllOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.operators.JavaUnionAllOperator;
import org.qcri.rheem.java.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link UnionAllOperator} to {@link JavaUnionAllOperator}.
 */
public class UnionAllToJavaUnionAllMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory(),
                JavaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "unionAll", new UnionAllOperator<>(DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final UnionAllOperator<?> originalOperator = (UnionAllOperator<?>) subplanMatch.getMatch("unionAll").getOperator();
            return new JavaUnionAllOperator<>(originalOperator.getInputType0()).at(epoch);
        }
    }
}
