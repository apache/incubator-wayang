package org.qcri.rheem.spark.operators;

import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.basic.operators.StdoutSink;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

/**
 * Implementation of the {@link LocalCallbackSink} operator for the Java platform.
 */
public class SparkStdoutSink<T> extends StdoutSink<T> implements SparkExecutionOperator {
    /**
     * Creates a new instance.
     *
     * @param type     type of the incoming elements
     */
    public SparkStdoutSink(DataSetType<T> type) {
        super(type);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        inputs[0].provideRdd().toLocalIterator().forEachRemaining(System.out::println);
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkStdoutSink<>(this.getType());
    }
}
