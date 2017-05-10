package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.JSONSource;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.java.operators.JavaJSONSource;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link JSONSource} to {@link JavaJSONSource}.
 */
public class JSONSourceMapping implements Mapping {

    public JSONSourceMapping() {
    }

    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), this.createReplacementSubplanFactory(), new Platform[]{JavaPlatform.getInstance()}));
    }

    private SubplanPattern createSubplanPattern() {
        OperatorPattern operatorPattern = new OperatorPattern("source", new JSONSource((String)null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<JSONSource>(
                (matchedOperator, epoch) -> new JavaJSONSource(matchedOperator).at(epoch)
        );
    }
}
