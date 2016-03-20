package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.MaterializedGroupByOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.operators.JavaMaterializedGroupByOperator;
import org.qcri.rheem.java.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link MaterializedGroupByOperator} to {@link JavaMaterializedGroupByOperator}.
 */
public class JavaCollocateByOperatorMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory(),
                JavaPlatform.getInstance()));
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
            return new JavaMaterializedGroupByOperator<>(
                    originalOperator.getKeyDescriptor().unchecked(),
                    originalOperator.getInputType().unchecked(),
                    originalOperator.getOutputType().uncheckedGroup()
            ).at(epoch);
        }
    }
}
