package org.qcri.rheem.flink.operators;

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.qcri.rheem.basic.operators.DoWhileOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.compiler.criterion.DummyFilter;
import org.qcri.rheem.flink.compiler.criterion.DummyMap;
import org.qcri.rheem.flink.compiler.criterion.RheemAggregator;
import org.qcri.rheem.flink.compiler.criterion.RheemFilterCriterion;
import org.qcri.rheem.flink.execution.FlinkExecutor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Flink implementation of the {@link DoWhileOperator}.
 */
public class FlinkDoWhileOperator<InputType, ConvergenceType>
        extends DoWhileOperator<InputType, ConvergenceType>
        implements FlinkExecutionOperator {

    private IterativeDataSet iterativeDataSet;
    /**
     * Creates a new instance.
     */
    public FlinkDoWhileOperator(DataSetType<InputType> inputType,
                               DataSetType<ConvergenceType> convergenceType,
                               PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate,
                               Integer numExpectedIterations) {
        super(inputType, convergenceType, criterionPredicate, numExpectedIterations);
    }

    public FlinkDoWhileOperator(DataSetType<InputType> inputType,
                               DataSetType<ConvergenceType> convergenceType,
                               PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor,
                               Integer numExpectedIterations) {
        super(inputType, convergenceType, criterionDescriptor, numExpectedIterations);
    }

    /**
     * Creates a new instance.
     */
    public FlinkDoWhileOperator(DoWhileOperator<InputType, ConvergenceType> that) {
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

                final ConvergenceCriterion rheemConvergeCriterion = flinkExecutor.getCompiler().compile(this.criterionDescriptor);


                this.iterativeDataSet = input_initial
                        .iterate( 100000)
                        .registerAggregationConvergenceCriterion(
                                "Iteration_"+this.getName(),
                                new RheemAggregator(),
                                rheemConvergeCriterion
                        );

                output_iteration.accept(this.iterativeDataSet, flinkExecutor);

                outputs[FINAL_OUTPUT_INDEX] = null;
                this.setState(State.RUNNING);
                break;
            case RUNNING:
                assert this.iterativeDataSet != null;

                DataSet<InputType> input_iteration = ((DataSetChannel.Instance) inputs[ITERATION_INPUT_INDEX]).provideDataSet();
                DataSet<ConvergenceType> input_convergence = ((DataSetChannel.Instance) inputs[CONVERGENCE_INPUT_INDEX]).provideDataSet();
                DataSetChannel.Instance final_output = ((DataSetChannel.Instance) outputs[FINAL_OUTPUT_INDEX]);


                Class<InputType> input_class = this.getInputType().getDataUnitType().getTypeClass();
                Class<ConvergenceType> convergence_class = this.getConvergenceType().getDataUnitType().getTypeClass();



                DataSet<ConvergenceType> dummy_start = input_iteration
                        .map(
                            new DummyMap<InputType, ConvergenceType>(
                                    input_class,
                                    convergence_class
                            )
                        )
                        .filter(
                            new DummyFilter<ConvergenceType>(
                                convergence_class
                            )
                        );

                DataSet<ConvergenceType> dummy_union = input_convergence.union(dummy_start);

                DataSet<ConvergenceType> filter = dummy_union.filter(new RheemFilterCriterion<>("Iteration_"+this.getName()));

                DataSet<InputType> dummy_finish = filter
                        .map(
                            new DummyMap<ConvergenceType, InputType>(
                                    convergence_class,
                                    input_class
                            )
                        )
                        .filter(
                            new DummyFilter<InputType>(
                                input_class
                            )
                        );

                DataSet<InputType> final_union = input_iteration.union(dummy_finish);

                final_output.accept(this.iterativeDataSet.closeWith(final_union), flinkExecutor);

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
        return "rheem.flink.while.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                FlinkExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.criterionDescriptor, configuration);
        return optEstimator;
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkDoWhileOperator<>(this.getInputType(),
                this.getConvergenceType(),
                this.getCriterionDescriptor().getJavaImplementation(),
                this.getNumExpectedIterations()
        );
    }


    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        switch (index) {
            case INITIAL_INPUT_INDEX:
            case ITERATION_INPUT_INDEX:
                return Collections.singletonList(DataSetChannel.DESCRIPTOR);
            case CONVERGENCE_INPUT_INDEX:
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
