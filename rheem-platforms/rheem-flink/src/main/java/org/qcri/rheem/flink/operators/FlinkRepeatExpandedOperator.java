package org.qcri.rheem.flink.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.qcri.rheem.basic.operators.RepeatOperator;
import org.qcri.rheem.core.api.exception.RheemException;
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
public class FlinkRepeatExpandedOperator<Type>
        extends RepeatOperator<Type>
        implements FlinkExecutionOperator  {
    /**
     * Keeps track of the current iteration number.
     */
    private int iterationCounter = 0;

    private int iteration_generate = 0;

    private int iteration_expanded = 0;

    int real_iteration;

    /**
     * Creates a new instance.
     */
    public FlinkRepeatExpandedOperator(int numIterations, DataSetType<Type> type) {
        super(numIterations, type);
        this.iteration_generate = this.getNumIterations();
    }

    public FlinkRepeatExpandedOperator(RepeatOperator<Type> that) {
        super(that);
        this.iteration_generate = this.getNumIterations();
    }

    private IterativeDataSet<Type> iterativeDataSet;

    private DataSetChannel.Instance previous;


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

        int expanded = (int)flinkExecutor.getConfiguration().getLongProperty("rheem.flink.maxExpanded");
        if(expanded > 64){
            throw new RheemException(String.format("the maxExpanded (%s) is more that 64.", expanded));
        }
        // TODO: see if the case have a more elements that after the expanded

        if(this.iteration_generate <= expanded){
            //final ExecutionLineageNode executionLineageNode = new ExecutionLineageNode(operatorContext);
            //executionLineageNode.addAtomicExecutionFromOperatorContext();

            DataSet<Type> input;
            switch (this.getState()) {
                case NOT_STARTED:
                    assert inputs[INITIAL_INPUT_INDEX] != null;
                    this.iterationCounter = 0;
                    input = ((DataSetChannel.Instance) inputs[INITIAL_INPUT_INDEX]).provideDataSet();
                    break;
                case RUNNING:
                    assert inputs[ITERATION_INPUT_INDEX] != null;
                    this.iterationCounter++;
                    input = ((DataSetChannel.Instance) inputs[ITERATION_INPUT_INDEX]).provideDataSet();
                    break;
                default:
                    throw new IllegalStateException(String.format("%s is finished, yet executed.", this));

            }

            if (this.iterationCounter >= this.getNumIterations()) {
                // final loop output
                ((DataSetChannel.Instance)outputs[FINAL_OUTPUT_INDEX]).accept(input, flinkExecutor);
                outputs[ITERATION_OUTPUT_INDEX] = null;
                this.setState(State.FINISHED);
            } else {
                outputs[FINAL_OUTPUT_INDEX] = null;
                ((DataSetChannel.Instance)outputs[ITERATION_OUTPUT_INDEX]).accept(input, flinkExecutor);
                this.setState(State.RUNNING);
            }
        }else {
            try {
                int real_iteration;
                switch (this.getState()) {
                    case NOT_STARTED:
                        DataSet<Type> input_initial = ((DataSetChannel.Instance) inputs[INITIAL_INPUT_INDEX]).provideDataSet();
                        DataSetChannel.Instance output_iteration = ((DataSetChannel.Instance) outputs[ITERATION_OUTPUT_INDEX]);
                        real_iteration =(this.getNumIterations() - (this.getNumIterations() % expanded)) / expanded;
                        this.iterativeDataSet = input_initial
                                .iterate(real_iteration)
                                .setParallelism(flinkExecutor.getNumDefaultPartitions());

                        output_iteration.accept(this.iterativeDataSet, flinkExecutor);
                        outputs[FINAL_OUTPUT_INDEX] = null;
                        this.iterationCounter = 0;

                        this.setState(State.RUNNING);
                        break;
                    case RUNNING:
                        assert this.iterativeDataSet != null;

                        if(this.iteration_expanded++ == expanded){
                            real_iteration = (this.getNumIterations() - (this.getNumIterations() % expanded)) / expanded;
                            this.iterationCounter = this.iterationCounter - (real_iteration * expanded);
                            DataSet<Type> input_iteration = ((DataSetChannel.Instance) inputs[ITERATION_INPUT_INDEX]).provideDataSet();
                            DataSetChannel.Instance output_final = ((DataSetChannel.Instance) outputs[FINAL_OUTPUT_INDEX]);
                            output_final.accept(this.iterativeDataSet.setParallelism(flinkExecutor.getNumDefaultPartitions()).closeWith(input_iteration), flinkExecutor);
                            outputs[ITERATION_OUTPUT_INDEX] = null;

                            //TODO: see if after the expanded have another case
                            //if(this.iterationCounter == 0) {
                            this.setState(State.FINISHED);
                            //}
                            break;
                        }


                        DataSet<Type> input_iteration = ((DataSetChannel.Instance) inputs[ITERATION_INPUT_INDEX]).provideDataSet();
                        DataSetChannel.Instance iteration_output = ((DataSetChannel.Instance) outputs[ITERATION_OUTPUT_INDEX]);
                        iteration_output.accept(input_iteration, flinkExecutor);
                        outputs[FINAL_OUTPUT_INDEX] = null;
                        this.setState(State.RUNNING);

                        break;
                    default:
                        throw new IllegalStateException(String.format("%s is finished, yet executed.", this));
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }
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
        return new FlinkRepeatExpandedOperator<>(this);
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
