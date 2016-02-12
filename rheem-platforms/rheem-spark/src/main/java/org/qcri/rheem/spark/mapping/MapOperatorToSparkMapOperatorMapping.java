package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.spark.operators.SparkMapOperator;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link MapOperator} to {@link SparkMapOperator}.
 */
public class MapOperatorToSparkMapOperatorMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "map", new MapOperator<>(null, null, null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final MapOperator<?, ?> originalOperator = (MapOperator<?, ?>) subplanMatch.getMatch("map").getOperator();
            return new SparkMapOperator(originalOperator.getInputType(),
                    originalOperator.getOutputType(),
                    originalOperator.getFunctionDescriptor()).at(epoch);
        }
    }
}
