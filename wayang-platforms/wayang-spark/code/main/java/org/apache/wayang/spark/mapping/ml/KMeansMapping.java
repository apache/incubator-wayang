package org.apache.wayang.spark.mapping.ml;

import org.apache.wayang.basic.operators.KMeansOperator;
import org.apache.wayang.core.mapping.*;
import org.apache.wayang.spark.operators.ml.SparkKMeansOperator;
import org.apache.wayang.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link KMeansOperator} to {@link SparkKMeansOperator}.
 */
@SuppressWarnings("unchecked")
public class KMeansMapping implements Mapping {

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
                "kMeans", new KMeansOperator(0), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<KMeansOperator>(
                (matchedOperator, epoch) -> new SparkKMeansOperator(matchedOperator).at(epoch)
        );
    }
}
