package org.apache.wayang.agoraeo.mappings.java;

import org.apache.wayang.agoraeo.operators.basic.Sen2CorWrapper;
import org.apache.wayang.agoraeo.operators.basic.SentinelSource;
import org.apache.wayang.agoraeo.operators.java.JavaSen2CorWrapper;
import org.apache.wayang.agoraeo.operators.java.JavaSentinelSource;
import org.apache.wayang.basic.operators.FlatMapOperator;
import org.apache.wayang.core.mapping.*;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

public class Sen2CorWrapperMapping implements Mapping {

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
                "sen2cor",
                new Sen2CorWrapper(
                        null,
                        null
                ),
                false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<Sen2CorWrapper>(
                (matchedOperator, epoch) -> new JavaSen2CorWrapper(matchedOperator).at(epoch)
        );
    }
}
