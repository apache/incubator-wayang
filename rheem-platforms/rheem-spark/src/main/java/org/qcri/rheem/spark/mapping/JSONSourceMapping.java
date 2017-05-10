package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.JSONSource;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.spark.operators.SparkJSONSource;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link JSONSource} to {@link SparkJSONSource}.
 */
public class JSONSourceMapping implements Mapping {

    public JSONSourceMapping() {
    }

    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), this.createReplacementSubplanFactory(), new Platform[]{SparkPlatform.getInstance()}));
    }

    private SubplanPattern createSubplanPattern() {
        OperatorPattern operatorPattern = new OperatorPattern("source", new JSONSource("", (String)null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<JSONSource>(
                (matchedOperator, epoch) -> new SparkJSONSource(matchedOperator).at(epoch)
        );
    }
}
