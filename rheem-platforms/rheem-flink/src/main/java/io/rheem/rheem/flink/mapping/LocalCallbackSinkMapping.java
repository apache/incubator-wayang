package io.rheem.rheem.flink.mapping;

import io.rheem.rheem.basic.operators.LocalCallbackSink;
import io.rheem.rheem.core.function.FunctionDescriptor;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.flink.operators.FlinkLocalCallbackSink;
import io.rheem.rheem.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link LocalCallbackSink} to {@link FlinkLocalCallbackSink}.
 */
public class LocalCallbackSinkMapping implements Mapping {
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
                "sink", new LocalCallbackSink<>((FunctionDescriptor.SerializableConsumer) null, DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<LocalCallbackSink>(
                (matchedOperator, epoch) -> new FlinkLocalCallbackSink<>(matchedOperator).at(epoch)
        );
    }
}
