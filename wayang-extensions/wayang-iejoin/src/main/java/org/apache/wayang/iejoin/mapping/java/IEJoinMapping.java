package org.apache.wayang.iejoin.mapping.java;

import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.iejoin.operators.IEJoinMasterOperator;
import org.apache.wayang.iejoin.operators.IEJoinOperator;
import org.apache.wayang.iejoin.operators.JavaIEJoinOperator;
import org.apache.wayang.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link IEJoinOperator} to {@link JavaIEJoinOperator}.
 */
public class IEJoinMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "iejoin", new IEJoinOperator<>(DataSetType.none(), DataSetType.none(), null, null, IEJoinMasterOperator.JoinCondition.GreaterThan, null, null, IEJoinMasterOperator.JoinCondition.GreaterThan), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<IEJoinOperator>(
                (matchedOperator, epoch) -> new JavaIEJoinOperator<>(
                        matchedOperator.getInputType0(),
                        matchedOperator.getInputType1(), matchedOperator.getGet0Pivot(), matchedOperator.getGet1Pivot(), matchedOperator.getCond0(), matchedOperator.getGet1Pivot(), matchedOperator.getGet1Ref(), matchedOperator.getCond1()
                ).at(epoch)
        );
    }
}
