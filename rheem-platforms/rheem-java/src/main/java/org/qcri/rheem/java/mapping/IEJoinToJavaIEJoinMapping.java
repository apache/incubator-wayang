package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.data.JoinCondition;
import org.qcri.rheem.basic.operators.IEJoinOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.java.operators.JavaIEJoinOperator;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link IEJoinOperator} to {@link JavaIEJoinOperator}.
 */
public class IEJoinToJavaIEJoinMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "iejoin", new IEJoinOperator<>(DataSetType.none(), DataSetType.none(), null, null, JoinCondition.GreaterThan, null, null, JoinCondition.GreaterThan), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<IEJoinOperator>(
                (matchedOperator, epoch) -> new JavaIEJoinOperator<>(
                        matchedOperator.getInputType0(),
                        matchedOperator.getInputType1(),matchedOperator.getGet0Pivot(), matchedOperator.getGet1Pivot(), matchedOperator.getCond0(), matchedOperator.getGet1Pivot(), matchedOperator.getGet1Ref(), matchedOperator.getCond1()
                ).at(epoch)
        );
    }
}
