package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

/**
 * Execution operator for the {@link SparkPlatform}.
 */
public interface SparkExecutionOperator extends ExecutionOperator {

    @Override
    default SparkPlatform getPlatform() {
        return SparkPlatform.getInstance();
    }

    /**
     * Evaluates this operator. Takes a set of {@link ChannelInstance}s according to the operator inputs and manipulates
     * a set of {@link ChannelInstance}s according to the operator outputs -- unless the operator is a sink, then it triggers
     * execution.
     *
     * @param inputs        {@link ChannelInstance}s that satisfy the inputs of this operator
     * @param outputs       {@link ChannelInstance}s that accept the outputs of this operator
     * @param compiler      compiles functions used by the operator
     * @param sparkExecutor {@link SparkExecutor} that executes this instance
     */
    void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor);

    /**
     * Utility method to name an RDD according to this instance's name.
     *
     * @param rdd that should be renamed
     * @see #getName()
     */
    default void name(JavaRDD<?> rdd) {
        if (this.getName() != null) {
            rdd.setName(this.getName());
        } else {
            rdd.setName(this.toString());
        }
    }

    /**
     * Utility method to name an RDD according to this instance's name.
     *
     * @param rdd that should be renamed
     * @see #getName()
     */
    default void name(JavaPairRDD<?, ?> rdd) {
        if (this.getName() != null) {
            rdd.setName(this.getName());
        } else {
            rdd.setName(this.toString());
        }
    }

}
