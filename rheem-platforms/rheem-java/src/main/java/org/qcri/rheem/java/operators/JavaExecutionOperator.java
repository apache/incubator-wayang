package org.qcri.rheem.java.operators;

import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.java.plugin.Activator;

import java.util.stream.Stream;

/**
 * Execution operator for the Java platform.
 */
public interface JavaExecutionOperator extends ExecutionOperator {

    @Override
    default Platform getPlatform() {
        return Activator.PLATFORM;
    }

    /**
     * Evaluates this operator. Takes a set of Java {@link Stream}s according to the operator inputs and produces
     * a set of {@link Stream}s according to the operator outputs -- unless the operator is a sink, then it triggers
     * execution.
     * @param inputStreams {@link Stream}s that satisfy the inputs of this operator
     * @return {@link Stream}s that statisfy the outputs of this operator
     */
    Stream[] evaluate(Stream[] inputStreams);

}
