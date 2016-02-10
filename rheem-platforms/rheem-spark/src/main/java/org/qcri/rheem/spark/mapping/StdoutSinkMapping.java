package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.basic.operators.StdoutSink;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.spark.operators.SparkLocalCallbackSink;
import org.qcri.rheem.spark.operators.SparkStdoutSink;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link LocalCallbackSink} to {@link SparkLocalCallbackSink}.
 */
public class StdoutSinkMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory(),
                SparkPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "sink", new StdoutSink<>(null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final StdoutSink originalSink = (StdoutSink) subplanMatch.getMatch("sink").getOperator();
            return new SparkStdoutSink<>(originalSink.getInput().getType()).at(epoch);
        }
    }
}
