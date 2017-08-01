package org.qcri.rheem.flink.mapping;

import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.basic.operators.TextFileSink;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.flink.operators.FlinkTextFileSink;
import org.qcri.rheem.flink.operators.FlinkTextFileSource;
import org.qcri.rheem.flink.platform.FlinkPlatform;
import org.qcri.rheem.java.operators.JavaTextFileSink;

import java.util.Collection;
import java.util.Collections;


/**
 * Mapping from {@link TextFileSink} to {@link FlinkTextFileSink}.
 */
public class TextFileSinkMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                FlinkPlatform.getInstance()
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
                (matchedOperator, epoch) -> new FlinkTextFileSink(matchedOperator).at(epoch)
        );
    }
}
