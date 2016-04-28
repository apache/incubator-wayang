package org.qcri.rheem.java.operators;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Execution operator for the Java platform.
 */
public interface JavaExecutionOperator extends ExecutionOperator {

    @Override
    default JavaPlatform getPlatform() {
        return JavaPlatform.getInstance();
    }

    /**
     * When this instance is not yet initialized, this method is called.
     *
     * @param inputs   {@link JavaChannelInstance}s that satisfy the inputs of this operator
     * @param compiler compiles functions used by this instance
     */
    default void open(JavaChannelInstance[] inputs, FunctionCompiler compiler) {
        // Do nothing by default.
    }

    /**
     * Evaluates this operator. Takes a set of Java {@link Stream}s according to the operator inputs and produces
     * a set of {@link Stream}s according to the operator outputs -- unless the operator is a sink, then it triggers
     * execution.
     *
     * @param inputs   {@link JavaChannelInstance}s that satisfy the inputs of this operator
     * @param outputs  {@link JavaChannelInstance}s that collect the outputs of this operator
     * @param compiler compiles functions used by the operator
     */
    void evaluate(JavaChannelInstance[] inputs, JavaChannelInstance[] outputs, FunctionCompiler compiler);

    default void instrumentSink(JavaChannelInstance channelExecutor) {
        throw new RheemException(String.format("Cannot instrument %s.", this));
    }
}
