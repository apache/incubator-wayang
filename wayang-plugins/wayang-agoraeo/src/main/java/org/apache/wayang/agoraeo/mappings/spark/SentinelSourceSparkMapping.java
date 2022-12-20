package org.apache.wayang.agoraeo.mappings.spark;

import org.apache.wayang.agoraeo.operators.basic.SentinelSource;
import org.apache.wayang.agoraeo.operators.spark.SparkSentinelSource;
import org.apache.wayang.core.mapping.*;
import org.apache.wayang.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

public class SentinelSourceSparkMapping implements Mapping {

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
                "source",
                new SentinelSource(

                ),
                false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SentinelSource>(
                (matchedOperator, epoch) -> new SparkSentinelSource(matchedOperator).at(epoch)
        );
    }
}
