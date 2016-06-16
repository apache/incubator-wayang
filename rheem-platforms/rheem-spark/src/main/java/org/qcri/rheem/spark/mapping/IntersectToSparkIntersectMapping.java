package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.IntersectOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.operators.SparkIntersectOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link IntersectOperator} to {@link SparkIntersectOperator}.
 */
public class IntersectToSparkIntersectMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory(),
                SparkPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "intersect", new IntersectOperator<>(DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final IntersectOperator<?> originalOperator = (IntersectOperator<?>) subplanMatch.getMatch("intersect").getOperator();
            return new SparkIntersectOperator<>(originalOperator.getInputType0()).at(epoch);
        }
    }
}
