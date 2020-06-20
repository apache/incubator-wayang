package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.operators.DoWhileOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
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
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Spark implementation of the {@link DoWhileOperator}.
 */
public class SparkDoWhileOperator<InputType, ConvergenceType>
        extends DoWhileOperator<InputType, ConvergenceType>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     */
    public SparkDoWhileOperator(DataSetType<InputType> inputType,
                                DataSetType<ConvergenceType> convergenceType,
                                PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate,
                                Integer numExpectedIterations) {
        super(inputType, convergenceType, criterionPredicate, numExpectedIterations);
    }

    public SparkDoWhileOperator(DataSetType<InputType> inputType,
                                DataSetType<ConvergenceType> convergenceType,
                                PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor,
                                Integer numExpectedIterations) {
        super(inputType, convergenceType, criterionDescriptor, numExpectedIterations);
    }

    /**
     * Creates a new instance.
     */
    public SparkDoWhileOperator(DoWhileOperator<InputType, ConvergenceType> that) {
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

        final RddChannel.Instance iterationInput;
        final Function<Collection<ConvergenceType>, Boolean> stoppingCondition =
                sparkExecutor.getCompiler().compile(this.criterionDescriptor, this, operatorContext, inputs);
        boolean endloop = false;
        switch (this.getState()) {
            case NOT_STARTED:
                assert inputs[INITIAL_INPUT_INDEX] != null;

                iterationInput = (RddChannel.Instance) inputs[INITIAL_INPUT_INDEX];
                break;
            case RUNNING:
                assert inputs[ITERATION_INPUT_INDEX] != null;
                assert inputs[CONVERGENCE_INPUT_INDEX] != null;

                iterationInput = (RddChannel.Instance) inputs[ITERATION_INPUT_INDEX];
                final CollectionChannel.Instance convergenceInput = (CollectionChannel.Instance) inputs[CONVERGENCE_INPUT_INDEX];
                final Collection<ConvergenceType> convergenceCollection = convergenceInput.provideCollection();
                try {
                    endloop = stoppingCondition.call(convergenceCollection);
                } catch (Exception e) {
                    throw new RheemException(String.format("Could not evaluate stopping condition for %s.", this), e);
                }
                executionLineageNode.addPredecessor(convergenceInput.getLineage());
                break;
            default:
                throw new IllegalStateException(String.format("%s is finished, yet executed.", this));

        }

        if (endloop) {
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
        return new SparkDoWhileOperator<>(
                this.getInputType(),
                this.getConvergenceType(),
                this.getCriterionDescriptor().getJavaImplementation(),
                this.getNumExpectedIterations()
        );
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.spark.while.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                SparkExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.criterionDescriptor, configuration);
        return optEstimator;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        switch (index) {
            case INITIAL_INPUT_INDEX:
            case ITERATION_INPUT_INDEX:
                return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
            case CONVERGENCE_INPUT_INDEX:
                return Collections.singletonList(CollectionChannel.DESCRIPTOR);
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
