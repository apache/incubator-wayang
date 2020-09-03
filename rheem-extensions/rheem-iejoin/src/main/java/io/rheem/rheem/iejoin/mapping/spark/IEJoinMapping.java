package io.rheem.rheem.iejoin.mapping.spark;

import io.rheem.rheem.basic.data.Record;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanMatch;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.core.plan.rheemplan.Operator;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.iejoin.operators.IEJoinMasterOperator;
import io.rheem.rheem.iejoin.operators.IEJoinOperator;
import io.rheem.rheem.iejoin.operators.SparkIEJoinOperator;
import io.rheem.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link IEJoinOperator} to {@link SparkIEJoinOperator}.
 */
public class IEJoinMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory(),
                SparkPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "iejoin", new IEJoinOperator<>(DataSetType.none(), DataSetType.none(), null, null, IEJoinMasterOperator.JoinCondition.GreaterThan, null, null, IEJoinMasterOperator.JoinCondition.GreaterThan), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory<InputType0 extends Record, InputType1 extends Record,
            Type0 extends Comparable<Type0>, Type1 extends Comparable<Type1>> extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final IEJoinOperator<?, ?, ?> originalOperator = (IEJoinOperator<?, ?, ?>) subplanMatch.getMatch("iejoin").getOperator();
            return new SparkIEJoinOperator(originalOperator.getInputType0(),
                    originalOperator.getInputType1(), originalOperator.getGet0Pivot(), originalOperator.getGet1Pivot(), originalOperator.getCond0(), originalOperator.getGet0Ref(), originalOperator.getGet1Ref(), originalOperator.getCond1()).at(epoch);
        }
    }
}
