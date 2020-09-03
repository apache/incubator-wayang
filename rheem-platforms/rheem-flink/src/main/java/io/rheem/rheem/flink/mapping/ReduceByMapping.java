package io.rheem.rheem.flink.mapping;

import io.rheem.rheem.basic.operators.ReduceByOperator;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.flink.operators.FlinkReduceByOperator;
import io.rheem.rheem.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link ReduceByOperator} to {@link FlinkReduceByOperator}.
 */
@SuppressWarnings("unchecked")
public class ReduceByMapping implements Mapping{
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                FlinkPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "reduceBy", new ReduceByOperator<>(null, null, DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<ReduceByOperator>(
                (matchedOperator, epoch) -> new FlinkReduceByOperator<>(matchedOperator).at(epoch)
        );
    }
}
