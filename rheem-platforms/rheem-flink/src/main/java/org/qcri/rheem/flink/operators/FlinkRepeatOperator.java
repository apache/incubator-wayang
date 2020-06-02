package org.qcri.rheem.flink.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.qcri.rheem.basic.operators.RepeatOperator;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.execution.FlinkExecutor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Flink implementation of the {@link RepeatOperator}.
 */
public class FlinkRepeatOperator<Type>
        extends RepeatOperator<Type>
        implements FlinkExecutionOperator  {
    /**
     * Keeps track of the current iteration number.
     */
    private int iterationCounter;

    /**
     * Creates a new instance.
     */
    public FlinkRepeatOperator(int numIterations, DataSetType<Type> type) {
        super(numIterations, type);
    }

    public FlinkRepeatOperator(RepeatOperator<Type> that) {
        super(that);
    }

    private IterativeDataSet<Type> iterativeDataSet;

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        assert inputs[INITIAL_INPUT_INDEX] != null;
        assert inputs[ITERATION_INPUT_INDEX] != null;

        assert outputs[ITERATION_OUTPUT_INDEX] != null;
        assert outputs[FINAL_OUTPUT_INDEX] != null;

        try {

            switch (this.getState()) {
                case NOT_STARTED:
                    DataSet<Type> input_initial = ((DataSetChannel.Instance) inputs[INITIAL_INPUT_INDEX]).provideDataSet();
                    DataSetChannel.Instance output_iteration = ((DataSetChannel.Instance) outputs[ITERATION_OUTPUT_INDEX]);

                    this.iterativeDataSet = input_initial
                                .iterate(this.getNumIterations())
                            .setParallelism(flinkExecutor.getNumDefaultPartitions());

                    output_iteration.accept(this.iterativeDataSet, flinkExecutor);
                    outputs[FINAL_OUTPUT_INDEX] = null;
                    this.iterationCounter = 0;

                    this.setState(State.RUNNING);
                    break;
                case RUNNING:
                    assert this.iterativeDataSet != null;
                    this.iterationCounter = this.getNumIterations();
                    DataSet<Type> input_iteration = ((DataSetChannel.Instance) inputs[ITERATION_INPUT_INDEX]).provideDataSet();
                    DataSetChannel.Instance output_final = ((DataSetChannel.Instance) outputs[FINAL_OUTPUT_INDEX]);
                    output_final.accept(this.iterativeDataSet.setParallelism(flinkExecutor.getNumDefaultPartitions()).closeWith(input_iteration), flinkExecutor);
                    outputs[ITERATION_OUTPUT_INDEX] = null;
                    this.setState(State.FINISHED);

                    break;
                default:
                    throw new IllegalStateException(String.format("%s is finished, yet executed.", this));
            }
        }catch (Exception e){
            e.printStackTrace();
            System.exit(0);
        }

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.repeat.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkRepeatOperator<>(this);
    }


    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        switch (index) {
            case INITIAL_INPUT_INDEX:
            case ITERATION_INPUT_INDEX:
                return Collections.singletonList(DataSetChannel.DESCRIPTOR);
            default:
                throw new IllegalStateException(String.format("%s has no %d-th input.", this, index));
        }
    }


    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

}
