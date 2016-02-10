package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.UnionAllOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.spark.operators.SparkUnionAllOperator;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link UnionAllOperator} to {@link SparkUnionAllOperator}.
 */
public class UnionAllToSparkUnionAllMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "unionAll", new UnionAllOperator<>(null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final UnionAllOperator<?> originalOperator = (UnionAllOperator<?>) subplanMatch.getMatch("unionAll").getOperator();
            return new SparkUnionAllOperator<>(originalOperator.getInputType0()).at(epoch);
        }
    }
}
