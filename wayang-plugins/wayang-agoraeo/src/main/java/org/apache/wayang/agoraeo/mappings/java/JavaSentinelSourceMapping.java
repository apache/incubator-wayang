package org.apache.wayang.agoraeo.mappings.java;

import org.apache.wayang.agoraeo.operators.basic.SentinelSource;
import org.apache.wayang.agoraeo.operators.java.JavaSentinelSource;
import org.apache.wayang.core.mapping.*;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

public class JavaSentinelSourceMapping  implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "source",
                new SentinelSource<>(
                        null,
                        DataSetType.none().getDataUnitType().getTypeClass()
                ),
                false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SentinelSource>(
                (matchedOperator, epoch) -> new JavaSentinelSource(matchedOperator).at(epoch)
        );
    }
}
