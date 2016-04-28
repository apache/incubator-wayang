package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.operators.DoWhileOperator;
import org.qcri.rheem.basic.operators.LoopOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.*;

/**
 * Spark implementation of the {@link LoopOperator}.
 */
public class SparkDoWhileOperator<InputType, ConvergenceType>
        extends DoWhileOperator<InputType, ConvergenceType>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     */
    public SparkDoWhileOperator(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType,
                                PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate) {
        super(inputType, convergenceType, criterionPredicate);
    }

    public SparkDoWhileOperator(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType,
                                PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor) {
        super(inputType, convergenceType, criterionDescriptor);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler,
                         SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final RddChannel.Instance iterationInput;
        final Function<Collection<ConvergenceType>, Boolean> stoppingCondition =
                compiler.compile(this.criterionDescriptor, this, inputs);
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
                break;
            default:
                throw new IllegalStateException(String.format("%s is finished, yet executed.", this));

        }

        if (endloop) {
            // final loop output
            ((RddChannel.Instance) outputs[FINAL_OUTPUT_INDEX]).accept(iterationInput.provideRdd(), sparkExecutor);
            outputs[ITERATION_OUTPUT_INDEX] = null;
            this.setState(State.FINISHED);
        } else {
            outputs[FINAL_OUTPUT_INDEX] = null;
            ((RddChannel.Instance) outputs[ITERATION_OUTPUT_INDEX]).accept(iterationInput.provideRdd(), sparkExecutor);
            this.setState(State.RUNNING);
        }

    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkDoWhileOperator<>(this.getInputType(), this.getConvergenceType(),
                this.getCriterionDescriptor().getJavaImplementation());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(Configuration configuration) {
        // NB: Not actually measured, adapted from the SparkCollectionSource.
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(3, 2, .8d, CardinalityEstimate.EMPTY_ESTIMATE, (inputCards, outputCards) -> 4000 * inputCards[0] + 6272516800L),
                new DefaultLoadEstimator(3, 2, .9d, CardinalityEstimate.EMPTY_ESTIMATE, (inputCards, outputCards) -> 10000),
                new DefaultLoadEstimator(3, 2, .9d, CardinalityEstimate.EMPTY_ESTIMATE, (inputCards, outputCards) -> 0),
                new DefaultLoadEstimator(3, 2, .9d, CardinalityEstimate.EMPTY_ESTIMATE, (inputCards, outputCards) -> Math.round(4.5d * inputCards[0] + 43000)),
                0.08d,
                1500
        );
        return Optional.of(mainEstimator);
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
}
