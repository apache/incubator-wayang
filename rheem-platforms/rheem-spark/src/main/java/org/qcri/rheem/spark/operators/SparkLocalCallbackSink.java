package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.function.Consumer;

/**
 * Implementation of the {@link LocalCallbackSink} operator for the Java platform.
 */
public class SparkLocalCallbackSink<T> extends LocalCallbackSink<T> implements SparkExecutionOperator {
    /**
     * Creates a new instance.
     *
     * @param callback callback that is executed locally for each incoming data unit
     * @param type     type of the incoming elements
     */
    public SparkLocalCallbackSink(Consumer<T> callback, DataSetType type) {
        super(callback, type);
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputRdds, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        final JavaRDDLike inputRdd = inputRdds[0];
        inputRdd.collect().forEach(this.callback);
        return new JavaRDDLike[0];
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkLocalCallbackSink<>(this.callback, this.getType());
    }
}
