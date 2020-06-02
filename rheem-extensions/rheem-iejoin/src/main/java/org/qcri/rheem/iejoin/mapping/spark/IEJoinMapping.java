package org.qcri.rheem.iejoin.mapping.spark;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.OperatorPattern;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanMatch;
import org.qcri.rheem.core.mapping.SubplanPattern;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.iejoin.operators.IEJoinMasterOperator;
import org.qcri.rheem.iejoin.operators.IEJoinOperator;
import org.qcri.rheem.iejoin.operators.SparkIEJoinOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

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
