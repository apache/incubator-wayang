package org.qcri.rheem.java.operators;

import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.plugin.JavaPlatform;

import java.util.stream.Stream;

/**
 * Execution operator for the Java platform.
 */
public interface JavaExecutionOperator extends ExecutionOperator {

    @Override
    default Platform getPlatform() {
        return JavaPlatform.getInstance();
    }

    /**
     * Evaluates this operator. Takes a set of Java {@link Stream}s according to the operator inputs and produces
     * a set of {@link Stream}s according to the operator outputs -- unless the operator is a sink, then it triggers
     * execution.
     *
     * @param inputStreams {@link Stream}s that satisfy the inputs of this operator
     * @param compiler     compiles functions used by the operator
     * @return {@link Stream}s that statisfy the outputs of this operator
     */
    Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler);

}
