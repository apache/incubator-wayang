package org.apache.incubator.wayang.flink.operators;

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.incubator.wayang.basic.operators.LoopOperator;
import org.apache.incubator.wayang.basic.operators.RepeatOperator;
import org.apache.incubator.wayang.core.function.PredicateDescriptor;
import org.apache.incubator.wayang.core.optimizer.OptimizationContext;
import org.apache.incubator.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.incubator.wayang.core.platform.ChannelDescriptor;
import org.apache.incubator.wayang.core.platform.ChannelInstance;
import org.apache.incubator.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.incubator.wayang.core.types.DataSetType;
import org.apache.incubator.wayang.core.util.Tuple;
import org.apache.incubator.wayang.flink.channels.DataSetChannel;
import org.apache.incubator.wayang.flink.compiler.criterion.WayangAggregator;
import org.apache.incubator.wayang.flink.compiler.criterion.WayangFilterCriterion;
import org.apache.incubator.wayang.flink.execution.FlinkExecutor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Flink implementation of the {@link RepeatOperator}.
 */
public class FlinkLoopOperator<InputType, ConvergenceType>
        extends LoopOperator<InputType, ConvergenceType>
        implements FlinkExecutionOperator  {

    private IterativeDataSet iterativeDataSet;

    /**
     * Creates a new instance.
     */
    public FlinkLoopOperator(DataSetType<InputType> inputType,
                            DataSetType<ConvergenceType> convergenceType,
                            PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate,
                            Integer numExpectedIterations) {
        super(inputType, convergenceType, criterionPredicate, numExpectedIterations);
    }

    public FlinkLoopOperator(DataSetType<InputType> inputType,
                            DataSetType<ConvergenceType> convergenceType,
                            PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor,
                            Integer numExpectedIterations) {
        super(inputType, convergenceType, criterionDescriptor, numExpectedIterations);
    }

    /**
     * Creates a copy of the given {@link LoopOperator}.
     *
     * @param that should be copied
     */
    public FlinkLoopOperator(LoopOperator<InputType, ConvergenceType> that) {
        super(that);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        switch (this.getState()) {
            case NOT_STARTED:
                DataSet<InputType> input_initial = ((DataSetChannel.Instance) inputs[INITIAL_INPUT_INDEX]).provideDataSet();
                DataSetChannel.Instance output_iteration = ((DataSetChannel.Instance) outputs[ITERATION_OUTPUT_INDEX]);

                final ConvergenceCriterion wayangConvergeCriterion = flinkExecutor.getCompiler().compile(this.criterionDescriptor);

                DataSet<InputType> initial_convergence = ((DataSetChannel.Instance) inputs[INITIAL_CONVERGENCE_INPUT_INDEX]).provideDataSet();
                DataSetChannel.Instance output_convergence = ((DataSetChannel.Instance) outputs[ITERATION_CONVERGENCE_OUTPUT_INDEX]);


                this.iterativeDataSet = input_initial
                        .iterate( 100)
                        .registerAggregationConvergenceCriterion(
                                "Iteration_"+this.getName(),
                                new WayangAggregator(),
                                wayangConvergeCriterion
                        );

                output_iteration.accept(this.iterativeDataSet, flinkExecutor);


                output_convergence.accept(initial_convergence, flinkExecutor);

                outputs[FINAL_OUTPUT_INDEX] = null;
                this.setState(State.RUNNING);
                break;
            case RUNNING:
                assert this.iterativeDataSet != null;

                DataSet<InputType> input_iteration = ((DataSetChannel.Instance) inputs[ITERATION_INPUT_INDEX]).provideDataSet();
                DataSetChannel.Instance final_output = ((DataSetChannel.Instance) outputs[FINAL_OUTPUT_INDEX]);


                DataSet<InputType> filter = input_iteration.filter(new WayangFilterCriterion<>("Iteration_"+this.getName()));


                final_output.accept(this.iterativeDataSet.closeWith(filter), flinkExecutor);

                outputs[ITERATION_OUTPUT_INDEX] = null;
                this.setState(State.FINISHED);

                break;
            default:
                throw new IllegalStateException(String.format("%s is finished, yet executed.", this));
        }

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.flink.loop.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkLoopOperator<>(this);
    }


    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        switch (index) {
            case INITIAL_INPUT_INDEX:
            case ITERATION_INPUT_INDEX:
                return Collections.singletonList(DataSetChannel.DESCRIPTOR);
            case INITIAL_CONVERGENCE_INPUT_INDEX:
            case ITERATION_CONVERGENCE_INPUT_INDEX:
                return Collections.singletonList(DataSetChannel.DESCRIPTOR);
            default:
                throw new IllegalStateException(String.format("%s has no %d-th input.", this, index));
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        switch (index) {
            case ITERATION_OUTPUT_INDEX:
            case FINAL_OUTPUT_INDEX:
                return Collections.singletonList(DataSetChannel.DESCRIPTOR);
            case INITIAL_CONVERGENCE_INPUT_INDEX:
            case ITERATION_CONVERGENCE_INPUT_INDEX:
                return Collections.singletonList(DataSetChannel.DESCRIPTOR);
            default:
                throw new IllegalStateException(String.format("%s has no %d-th input.", this, index));
        }
    }

}
