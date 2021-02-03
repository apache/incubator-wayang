package org.apache.wayang.iejoin.mapping.spark;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanMatch;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.iejoin.operators.IEJoinMasterOperator;
import org.apache.wayang.iejoin.operators.IEJoinOperator;
import org.apache.wayang.iejoin.operators.SparkIEJoinOperator;
import org.apache.wayang.spark.platform.SparkPlatform;

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
