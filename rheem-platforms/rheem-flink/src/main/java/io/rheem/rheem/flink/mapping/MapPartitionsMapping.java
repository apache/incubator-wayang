package io.rheem.rheem.flink.mapping;

import io.rheem.rheem.basic.operators.MapPartitionsOperator;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.mapping.OperatorPattern;
import io.rheem.rheem.core.mapping.PlanTransformation;
import io.rheem.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.rheem.core.mapping.SubplanPattern;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.flink.operators.FlinkMapPartitionsOperator;
import io.rheem.rheem.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link MapPartitionsOperator} to {@link FlinkMapPartitionsOperator}.
 */
@SuppressWarnings("unchecked")
public class MapPartitionsMapping implements Mapping{

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singletonList(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        FlinkPlatform.getInstance()
                )
        );
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "mapPartitions", new MapPartitionsOperator<>(null, DataSetType.none(), DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<MapPartitionsOperator>(
                (matchedOperator, epoch) -> new FlinkMapPartitionsOperator<>(matchedOperator).at(epoch)
        );
    }
}
