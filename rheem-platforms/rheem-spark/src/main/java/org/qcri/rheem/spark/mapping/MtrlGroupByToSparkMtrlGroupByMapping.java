package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.MaterializedGroupByOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.operators.SparkMaterializedGroupByOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link MaterializedGroupByOperator} to {@link SparkMaterializedGroupByOperator}.
 */
public class MtrlGroupByToSparkMtrlGroupByMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory(),
                SparkPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "operator", new MaterializedGroupByOperator<>(null, DataSetType.none(), DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final MaterializedGroupByOperator<?, ?> originalOperator = (MaterializedGroupByOperator<?, ?>) subplanMatch.getMatch("operator").getOperator();
            return new SparkMaterializedGroupByOperator<>(
                    originalOperator.getKeyDescriptor().unchecked(),
                    originalOperator.getInputType().unchecked(),
                    originalOperator.getOutputType().uncheckedGroup()
            ).at(epoch);
        }
    }
}
