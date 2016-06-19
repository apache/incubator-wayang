package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.data.JoinCondition;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.IESelfJoinOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.operators.SparkIESelfJoinOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link IESelfJoinOperator} to {@link SparkIESelfJoinOperator}.
 */
public class IESelfJoinToSparkIEJoinMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory(),
                SparkPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "ieselfjoin", new IESelfJoinOperator<>(DataSetType.none(),  0,JoinCondition.GreaterThan, 0,JoinCondition.GreaterThan), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory<InputType0 extends Record, InputType1 extends Record,
            Type0 extends Comparable<Type0>, Type1 extends Comparable<Type1>> extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final IESelfJoinOperator<?, ?> originalOperator = (IESelfJoinOperator<?, ?>) subplanMatch.getMatch("ieselfjoin").getOperator();
            return new SparkIESelfJoinOperator<>(originalOperator.getInputType(), 0, JoinCondition.GreaterThan, 0,JoinCondition.GreaterThan).at(epoch);
        }
    }
}
