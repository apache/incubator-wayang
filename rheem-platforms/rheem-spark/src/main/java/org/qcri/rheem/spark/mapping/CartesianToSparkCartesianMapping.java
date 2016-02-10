package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.CartesianOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.spark.operators.SparkCartesianOperator;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link CartesianOperator} to {@link SparkCartesianOperator}.
 */
public class CartesianToSparkCartesianMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "cartesian", new CartesianOperator<>(null, null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final CartesianOperator<?, ?> originalOperator = (CartesianOperator<?, ?>) subplanMatch.getMatch("cartesian").getOperator();
            return new SparkCartesianOperator<>(originalOperator.getInputType0(),
                    originalOperator.getInputType1()).at(epoch);
        }
    }
}
