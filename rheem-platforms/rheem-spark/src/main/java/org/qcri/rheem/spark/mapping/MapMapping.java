package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.operators.SparkMapOperator;
import org.qcri.rheem.spark.operators.SparkMapPartitionsOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Arrays;
import java.util.Collection;

/**
 * Mapping from {@link MapOperator} to {@link SparkMapOperator}.
 */
@SuppressWarnings("unchecked")
public class MapMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Arrays.asList(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createSparkMapReplacementSubplanFactory(),
                        SparkPlatform.getInstance()
                ),
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createSparkMapPartitionsReplacementSubplanFactory(),
                        SparkPlatform.getInstance()
                )
        );
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "map", new MapOperator<>(null, DataSetType.none(), DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createSparkMapReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<MapOperator>(
                (matchedOperator, epoch) -> new SparkMapOperator<>(matchedOperator).at(epoch)
        );
    }

    private ReplacementSubplanFactory createSparkMapPartitionsReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<MapOperator>(
                (matchedOperator, epoch) -> new SparkMapPartitionsOperator<>(matchedOperator).at(epoch)
        );
    }
}
