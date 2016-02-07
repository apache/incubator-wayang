package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.plugin.Activator;

import org.apache.spark.api.java.JavaRDDLike;

/**
 * Execution operator for the Java platform.
 */
public interface SparkExecutionOperator extends ExecutionOperator {

    @Override
    default Platform getPlatform() {
        return Activator.PLATFORM;
    }


    default JavaSparkContext getSC() {return Activator.sc;};

    /**
     * Evaluates this operator. Takes a set of Java {@link JavaRDDLike}s according to the operator inputs and produces
     * a set of {@link JavaRDDLike}s according to the operator outputs -- unless the operator is a sink, then it triggers
     * execution.
     *
     * @param inputStreams {@link JavaRDDLike}s that satisfy the inputs of this operator
     * @param compiler     compiles functions used by the operator
     * @return {@link JavaRDDLike}s that statisfy the outputs of this operator
     */
    JavaRDDLike[] evaluate(JavaRDDLike[] inputStreams, FunctionCompiler compiler);

}
