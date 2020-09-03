package io.rheem.rheem.spark.operators;

import io.rheem.rheem.basic.operators.RepeatOperator;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.util.Tuple;
import io.rheem.rheem.spark.channels.RddChannel;
import io.rheem.rheem.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Spark implementation of the {@link RepeatOperator}.
 */
public class SparkRepeatOperator<Type>
        extends RepeatOperator<Type>
        implements SparkExecutionOperator {

    /**
     * Keeps track of the current iteration number.
     */
    private int iterationCounter;

    public SparkRepeatOperator(int numIterations, DataSetType<Type> type) {
        super(numIterations, type);
    }

    public SparkRepeatOperator(RepeatOperator<Type> that) {
        super(that);
    }


    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        ExecutionLineageNode executionLineageNode = new ExecutionLineageNode(operatorContext);
        executionLineageNode.addAtomicExecutionFromOperatorContext();

        RddChannel.Instance iterationInput;
        switch (this.getState()) {
            case NOT_STARTED:
                assert inputs[INITIAL_INPUT_INDEX] != null;
                iterationInput = (RddChannel.Instance) inputs[INITIAL_INPUT_INDEX];
                this.iterationCounter = 0;
                break;
            case RUNNING:
                assert inputs[ITERATION_INPUT_INDEX] != null;
                iterationInput = (RddChannel.Instance) inputs[ITERATION_INPUT_INDEX];
                this.iterationCounter++;
                break;
            default:
                throw new IllegalStateException(String.format("%s is finished, yet executed.", this));

        }

        if (this.iterationCounter >= this.getNumIterations()) {
            // final loop output
            sparkExecutor.forward(iterationInput, outputs[FINAL_OUTPUT_INDEX]);
            outputs[ITERATION_OUTPUT_INDEX] = null;
            this.setState(State.FINISHED);
        } else {
            outputs[FINAL_OUTPUT_INDEX] = null;
            sparkExecutor.forward(iterationInput, outputs[ITERATION_OUTPUT_INDEX]);
            this.setState(State.RUNNING);
        }

        return executionLineageNode.collectAndMark();
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkRepeatOperator<>(this);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.spark.repeat.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        switch (index) {
            case INITIAL_INPUT_INDEX:
            case ITERATION_INPUT_INDEX:
                return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
            default:
                throw new IllegalStateException(String.format("%s has no %d-th input.", this, index));
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
        // TODO: In this specific case, the actual output Channel is context-sensitive because we could forward Streams/Collections.
    }

    @Override
    public boolean containsAction() {
        return false;
    }

}
