package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.basic.operators.TextFileSink;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.OperatorPattern;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanPattern;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.operators.SparkCollectionSource;
import org.qcri.rheem.spark.operators.SparkTextFileSink;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link CollectionSource} to {@link SparkCollectionSource}.
 */
public class TextFileSinkMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                SparkPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "sink", new TextFileSink<>("", DataSetType.none().getDataUnitType().getTypeClass()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<TextFileSink<?>>(
                (matchedOperator, epoch) -> new SparkTextFileSink<>(matchedOperator).at(epoch)
        );
    }
}
