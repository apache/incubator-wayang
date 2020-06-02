package org.qcri.rheem.iejoin.mapping.java;

import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.OperatorPattern;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanPattern;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.iejoin.operators.IEJoinMasterOperator;
import org.qcri.rheem.iejoin.operators.IESelfJoinOperator;
import org.qcri.rheem.iejoin.operators.JavaIESelfJoinOperator;
import org.qcri.rheem.java.platform.JavaPlatform;

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
