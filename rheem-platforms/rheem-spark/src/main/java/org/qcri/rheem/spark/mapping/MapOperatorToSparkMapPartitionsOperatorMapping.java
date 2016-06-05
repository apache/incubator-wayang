package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.operators.SparkMapOperator;
import org.qcri.rheem.spark.operators.SparkMapPartitionsOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link MapOperator} to {@link SparkMapOperator}.
 */
@SuppressWarnings("unchecked")
public class MapOperatorToSparkMapPartitionsOperatorMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        SparkPlatform.getInstance()
                )
        );
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "map", new MapOperator<>(null, DataSetType.none(), DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<MapOperator>(
                (matchedOperator, epoch) -> new SparkMapPartitionsOperator<>(
                        matchedOperator.getFunctionDescriptor(),
                        matchedOperator.getInputType(),
                        matchedOperator.getOutputType()
                ).at(epoch)
        );
    }
}
