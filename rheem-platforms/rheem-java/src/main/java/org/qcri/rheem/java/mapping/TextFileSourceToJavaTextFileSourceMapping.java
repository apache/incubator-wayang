package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.java.operators.JavaTextFileSource;
import org.qcri.rheem.java.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link TextFileSource} to {@link JavaTextFileSource}.
 */
public class TextFileSourceToJavaTextFileSourceMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory(),
                JavaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "source", new org.qcri.rheem.basic.operators.TextFileSource(null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final TextFileSource originalSource = (TextFileSource) subplanMatch.getMatch("source").getOperator();
            return new JavaTextFileSource(originalSource.getInputUrl()).at(epoch);
        }
    }
}
