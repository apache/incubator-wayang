package org.qcri.rheem.java.mapping;

import org.qcri.rheem.basic.operators.StdoutSink;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.operators.JavaStdoutSink;
import org.qcri.rheem.java.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link StdoutSink} to {@link JavaStdoutSink}.
 */
public class StdoutSinkToJavaStdoutSinkMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory(),
                JavaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "sink", new StdoutSink<>(DataSetType.createDefault(Void.class)), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final StdoutSink originalSink = (StdoutSink) subplanMatch.getMatch("sink").getOperator();
            return new JavaStdoutSink<>(originalSink.getInput().getType()).at(epoch);
        }
    }
}
