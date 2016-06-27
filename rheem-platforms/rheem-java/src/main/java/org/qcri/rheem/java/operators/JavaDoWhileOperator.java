package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.DoWhileOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.*;
import java.util.function.Predicate;

/**
 * Java implementation of the {@link DoWhileOperator}.
 */
public class JavaDoWhileOperator<InputType, ConvergenceType>
        extends DoWhileOperator<InputType, ConvergenceType>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     */
    public JavaDoWhileOperator(DataSetType<InputType> inputType,
                               DataSetType<ConvergenceType> convergenceType,
                               PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate,
                               Integer numExpectedIterations) {
        super(inputType, convergenceType, criterionPredicate, numExpectedIterations);
    }

    public JavaDoWhileOperator(DataSetType<InputType> inputType,
                               DataSetType<ConvergenceType> convergenceType,
                               PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor,
                               Integer numExpectedIterations) {
        super(inputType, convergenceType, criterionDescriptor, numExpectedIterations);
    }

    @Override
    public void open(ChannelInstance[] inputs, FunctionCompiler compiler) {
        final Predicate<Collection<ConvergenceType>> udf = compiler.compile(this.criterionDescriptor);
        JavaExecutor.openFunction(this, udf, inputs);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Predicate<Collection<ConvergenceType>> stoppingCondition = compiler.compile(this.criterionDescriptor);
        boolean endloop = false;

        final Collection<ConvergenceType> convergenceCollection;
        final JavaChannelInstance input;
        switch (this.getState()) {
            case NOT_STARTED:
                assert inputs[INITIAL_INPUT_INDEX] != null;

                input = (JavaChannelInstance) inputs[INITIAL_INPUT_INDEX];
                break;
            case RUNNING:
                assert inputs[ITERATION_INPUT_INDEX] != null;
                assert inputs[CONVERGENCE_INPUT_INDEX] != null;

                convergenceCollection = ((CollectionChannel.Instance) inputs[CONVERGENCE_INPUT_INDEX]).provideCollection();
                endloop = stoppingCondition.test(convergenceCollection);
                input = (JavaChannelInstance) inputs[ITERATION_INPUT_INDEX];
                break;
            default:
                throw new IllegalStateException(String.format("%s is finished, yet executed.", this));

        }

        if (endloop) {
            // final loop output
            this.forward(input, (JavaChannelInstance) outputs[FINAL_OUTPUT_INDEX]);
            outputs[ITERATION_OUTPUT_INDEX] = null;
            this.setState(State.FINISHED);
        } else {
            outputs[FINAL_OUTPUT_INDEX] = null;
            this.forward(input, (JavaChannelInstance) outputs[ITERATION_OUTPUT_INDEX]);
            this.setState(State.RUNNING);
        }
    }

    private void forward(JavaChannelInstance input, JavaChannelInstance output) {
        ((StreamChannel.Instance) output).accept(input.provideStream());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(Configuration configuration) {
        final NestableLoadProfileEstimator estimator = NestableLoadProfileEstimator.parseSpecification(
                configuration.getStringProperty("rheem.java.while.load")
        );
        return Optional.of(estimator);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaDoWhileOperator<>(this.getInputType(),
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
                return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
            case CONVERGENCE_INPUT_INDEX:
                return Collections.singletonList(CollectionChannel.DESCRIPTOR);
            default:
                throw new IllegalStateException(String.format("%s has no %d-th input.", this, index));
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
        // TODO: In this specific case, the actual output Channel is context-sensitive because we could forward Streams/Collections.
    }
}
