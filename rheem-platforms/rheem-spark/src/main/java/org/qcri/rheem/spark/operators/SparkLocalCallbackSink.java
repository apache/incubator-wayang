package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.Optional;
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
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final JavaRDD<T> inputRdd = inputs[0].provideRdd();
        inputRdd.toLocalIterator().forEachRemaining(this.callback::accept);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkLocalCallbackSink<>(this.callback, this.getType());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(1, 0, .9d, (inputCards, outputCards) -> 4000 * inputCards[0] + 6272516800L),
                new DefaultLoadEstimator(1, 0, .9d, (inputCards, outputCards) -> 10000),
                new DefaultLoadEstimator(1, 0, .9d, (inputCards, outputCards) -> 0),
                new DefaultLoadEstimator(1, 0, .9d, (inputCards, outputCards) -> Math.round(4.5d * inputCards[0] + 43000)),
                0.08d,
                1000
        );

        return Optional.of(mainEstimator);
    }
}
