package org.apache.wayang.agoraeo.mappings.spark;

import org.apache.wayang.agoraeo.operators.basic.Sen2CorWrapper;
import org.apache.wayang.agoraeo.operators.java.JavaSen2CorWrapper;
import org.apache.wayang.agoraeo.operators.spark.SparkSen2CorWrapper;
import org.apache.wayang.core.mapping.*;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.platform.SparkPlatform;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

public class Sen2CorWrapperSparkMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                SparkPlatform.getInstance()
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
                (matchedOperator, epoch) -> new SparkSen2CorWrapper(matchedOperator).at(epoch)
        );
    }
}
