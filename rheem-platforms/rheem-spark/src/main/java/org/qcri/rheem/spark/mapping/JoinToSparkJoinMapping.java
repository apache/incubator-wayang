package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.operators.SparkJoinOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.qcri.rheem.core.function.TransformationDescriptor;
import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link JoinOperator} to {@link SparkJoinOperator}.
 */
public class JoinToSparkJoinMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory(),
                SparkPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "join", new JoinOperator<>(null,null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final JoinOperator<?, ?, ?> originalOperator = (JoinOperator<?, ?, ?>) subplanMatch.getMatch("join").getOperator();
            return new SparkJoinOperator<>
                    (originalOperator.getInputType0(),originalOperator.getInputType1(),
                            (TransformationDescriptor) originalOperator.getKeyDescriptor0(),(TransformationDescriptor)originalOperator.getKeyDescriptor1()).at(epoch);
        }
    }
}
