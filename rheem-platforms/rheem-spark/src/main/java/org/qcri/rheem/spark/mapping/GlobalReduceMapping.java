package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.GlobalReduceOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.spark.operators.SparkGlobalReduceOperator;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link GlobalReduceOperator} to {@link SparkGlobalReduceOperator}.
 * todo
 */
public class GlobalReduceMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "reduce", new GlobalReduceOperator<>(null, null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final GlobalReduceOperator<?> originalOperator = (GlobalReduceOperator<?>) subplanMatch.getMatch("reduce").getOperator();
            return new SparkGlobalReduceOperator<>(
                    originalOperator.getType().unchecked(),
                    originalOperator.getReduceDescriptor().unchecked()
            ).at(epoch);
        }
    }
}
