package org.apache.incubator.wayang.iejoin.mapping.java;

import org.apache.incubator.wayang.core.mapping.Mapping;
import org.apache.incubator.wayang.core.mapping.OperatorPattern;
import org.apache.incubator.wayang.core.mapping.PlanTransformation;
import org.apache.incubator.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.incubator.wayang.core.mapping.SubplanPattern;
import org.apache.incubator.wayang.core.types.DataSetType;
import org.apache.incubator.wayang.iejoin.operators.IEJoinMasterOperator;
import org.apache.incubator.wayang.iejoin.operators.IESelfJoinOperator;
import org.apache.incubator.wayang.iejoin.operators.JavaIESelfJoinOperator;
import org.apache.incubator.wayang.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link IESelfJoinOperator} to {@link JavaIESelfJoinOperator}.
 */
public class IESelfJoinMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "ieselfjoin", new IESelfJoinOperator<>(DataSetType.none(), null, IEJoinMasterOperator.JoinCondition.GreaterThan, null, IEJoinMasterOperator.JoinCondition.GreaterThan), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<IESelfJoinOperator>(
                (matchedOperator, epoch) -> new JavaIESelfJoinOperator<>(
                        matchedOperator.getInputType(),
                        matchedOperator.getGet0Pivot(),
                        matchedOperator.getCond0(),
                        matchedOperator.getGet0Ref(),
                        matchedOperator.getCond1()
                ).at(epoch)
        );
    }
}
