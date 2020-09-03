package io.rheem.rheem.iejoin.mapping.java;

import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.iejoin.operators.IEJoinMasterOperator;
import io.rheem.rheem.iejoin.operators.IESelfJoinOperator;
import io.rheem.rheem.iejoin.operators.JavaIESelfJoinOperator;
import io.rheem.rheem.java.platform.JavaPlatform;

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
