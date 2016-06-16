package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.operators.SparkJoinOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link JoinOperator} to {@link SparkJoinOperator}.
 */
public class JoinToSparkJoinMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(), this.createReplacementSubplanFactory(), SparkPlatform.getInstance())
        );
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "join", new JoinOperator<>(DataSetType.none(), DataSetType.none(), null, null), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<JoinOperator<Object, Object, Object>>(
                (matchedOperator, epoch) -> new SparkJoinOperator<>(
                        matchedOperator.getInputType0(),
                        matchedOperator.getInputType1(),
                        matchedOperator.getKeyDescriptor0(),
                        matchedOperator.getKeyDescriptor1()
                ).at(epoch)
        );
    }
}
