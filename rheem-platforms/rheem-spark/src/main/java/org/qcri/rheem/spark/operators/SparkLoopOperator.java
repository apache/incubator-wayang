package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.operators.LoopOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.PredicateDescriptor;
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
public class SparkLoopOperator<InputType, ConvergenceType>
        extends LoopOperator<InputType, ConvergenceType>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     */
    public SparkLoopOperator(DataSetType<InputType> inputType,
                             DataSetType<ConvergenceType> convergenceType,
                             PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate,
                             int numExpectedIterations) {
        super(inputType, convergenceType, criterionPredicate, numExpectedIterations);
    }

    public SparkLoopOperator(DataSetType<InputType> inputType,
                             DataSetType<ConvergenceType> convergenceType,
                             PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor,
                             int numExpectedIterations) {
        super(inputType, convergenceType, criterionDescriptor, numExpectedIterations);
    }

    /**
     * Creates a copy of the given {@link LoopOperator}.
     *
     * @param that should be copied
     */
    public SparkLoopOperator(LoopOperator<InputType, ConvergenceType> that) {
        super(that);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler,
                         SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final RddChannel.Instance iterationInput;
        final CollectionChannel.Instance convergenceInput;

        final Function<Collection<ConvergenceType>, Boolean> stoppingCondition =
                compiler.compile(this.criterionDescriptor, this, inputs);
        boolean endloop = false;

        switch (this.getState()) {
            case NOT_STARTED:
                assert inputs[INITIAL_INPUT_INDEX] != null;
                assert inputs[INITIAL_CONVERGENCE_INPUT_INDEX] != null;

                iterationInput = (RddChannel.Instance) inputs[INITIAL_INPUT_INDEX];
                convergenceInput = (CollectionChannel.Instance) inputs[INITIAL_CONVERGENCE_INPUT_INDEX];
                break;
            case RUNNING:
                assert inputs[ITERATION_INPUT_INDEX] != null;
                assert inputs[ITERATION_CONVERGENCE_INPUT_INDEX] != null;

                iterationInput = (RddChannel.Instance) inputs[ITERATION_INPUT_INDEX];
                convergenceInput = (CollectionChannel.Instance) inputs[ITERATION_CONVERGENCE_INPUT_INDEX];
                Collection<ConvergenceType> convergenceCollection = convergenceInput.provideCollection();
                try {
                    endloop = stoppingCondition.call(convergenceCollection);
                } catch (Exception e) {
                    throw new RheemException(
                            String.format("Could not evaluate stopping condition for %s.", this),
                            e
                    );
                }
                break;
            default:
                throw new IllegalStateException(String.format("%s is finished, yet executed.", this));

        }

        if (endloop) {
            // final loop output
            ((RddChannel.Instance) outputs[FINAL_OUTPUT_INDEX]).accept(iterationInput.provideRdd(), sparkExecutor);
            outputs[ITERATION_OUTPUT_INDEX] = null;
            outputs[ITERATION_CONVERGENCE_OUTPUT_INDEX] = null;
            this.setState(State.FINISHED);
        } else {
            outputs[FINAL_OUTPUT_INDEX] = null;
            ((RddChannel.Instance) outputs[ITERATION_OUTPUT_INDEX]).accept(iterationInput.provideRdd(), sparkExecutor);
            // We do not use forward(...) because we might not be able to consume the input JavaChannelInstance twice.
            ((CollectionChannel.Instance) outputs[ITERATION_CONVERGENCE_OUTPUT_INDEX]).accept(convergenceInput.provideCollection());
            this.setState(State.RUNNING);
        }

    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkLoopOperator<>(
                this.getInputType(),
                this.getConvergenceType(),
                this.getCriterionDescriptor().getJavaImplementation(),
                this.getNumExpectedIterations()
        );
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final String specification = configuration.getStringProperty("rheem.spark.loop.load");
        final NestableLoadProfileEstimator mainEstimator = NestableLoadProfileEstimator.parseSpecification(specification);
        final LoadProfileEstimator udfEstimator = configuration
                .getFunctionLoadProfileEstimatorProvider()
                .provideFor(this.criterionDescriptor);
        mainEstimator.nest(udfEstimator);
        return Optional.of(mainEstimator);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        switch (index) {
            case INITIAL_INPUT_INDEX:
            case ITERATION_INPUT_INDEX:
                return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
            case INITIAL_CONVERGENCE_INPUT_INDEX:
            case ITERATION_CONVERGENCE_INPUT_INDEX:
                return Collections.singletonList(CollectionChannel.DESCRIPTOR);
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
                // TODO: In this specific case, the actual output Channel is context-sensitive because we could forward Streams/Collections.
                return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
            case INITIAL_CONVERGENCE_INPUT_INDEX:
            case ITERATION_CONVERGENCE_INPUT_INDEX:
                return Collections.singletonList(CollectionChannel.DESCRIPTOR);
            default:
                throw new IllegalStateException(String.format("%s has no %d-th input.", this, index));
        }
    }
    
    @Override
    public boolean isExecutedEagerly() {
        return false;
    }

    @Override
    public boolean isEvaluatingEagerly(int inputIndex) {
        return inputIndex == INITIAL_CONVERGENCE_INPUT_INDEX || inputIndex == ITERATION_CONVERGENCE_INPUT_INDEX;
    }
}
