package org.qcri.rheem.java.operators;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.platform.JavaPlatform;

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
     * @param inputs          {@link JavaChannelInstance}s that satisfy the inputs of this operator
     * @param operatorContext contains optimization information for this instance
     * @param compiler        compiles functions used by this instance
     */
    default void open(ChannelInstance[] inputs,
                      OptimizationContext.OperatorContext operatorContext,
                      FunctionCompiler compiler) {
        // Do nothing by default.
    }

    /**
     * Evaluates this operator. Takes a set of Java {@link Stream}s according to the operator inputs and produces
     * a set of {@link Stream}s according to the operator outputs -- unless the operator is a sink, then it triggers
     * execution.
     *
     * @param inputs          {@link ChannelInstance}s that satisfy the inputs of this operator
     * @param outputs         {@link ChannelInstance}s that collect the outputs of this operator
     * @param javaExecutor    that executes this instance
     * @param operatorContext optimization information for this instance
     */
    void evaluate(ChannelInstance[] inputs,
                  ChannelInstance[] outputs,
                  JavaExecutor javaExecutor,
                  OptimizationContext.OperatorContext operatorContext);

}
