package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.LoopOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Java implementation of the {@link LoopOperator}.
 */
public class JavaLoopOperator<InputType, ConvergenceType>
        extends LoopOperator<InputType, ConvergenceType>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     */
    public JavaLoopOperator(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType,
                            PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate) {
        super(inputType, convergenceType, criterionPredicate);
    }

    public JavaLoopOperator(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType,
                            PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor) {
        super(inputType, convergenceType, criterionDescriptor);
    }

    @Override
    public void open(ChannelExecutor[] inputs, FunctionCompiler compiler) {
        final Predicate<Collection<ConvergenceType>> udf = compiler.compile(this.criterionDescriptor);
        JavaExecutor.openFunction(this, udf, inputs);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Predicate<Collection<ConvergenceType>> stoppingCondition = compiler.compile(this.criterionDescriptor);
        boolean endloop = false;

        final Collection<ConvergenceType> convergenceCollection;
        final ChannelExecutor input;
        switch (this.getState()) {
            case NOT_STARTED:
                assert inputs[INITIAL_INPUT_INDEX] != null;
                assert inputs[INITIAL_CONVERGENCE_INPUT_INDEX] != null;

                input = inputs[INITIAL_INPUT_INDEX];
                convergenceCollection = getAsCollection(inputs[INITIAL_CONVERGENCE_INPUT_INDEX]);
                break;
            case RUNNING:
                assert inputs[ITERATION_INPUT_INDEX] != null;
                assert inputs[ITERATION_CONVERGENCE_INPUT_INDEX] != null;

                convergenceCollection = getAsCollection(inputs[ITERATION_CONVERGENCE_INPUT_INDEX]);
                endloop = stoppingCondition.test(convergenceCollection);
                input = inputs[ITERATION_INPUT_INDEX];
                break;
            default:
                throw new IllegalStateException(String.format("%s is finished, yet executed.", this));

        }

        if (endloop) {
            // final loop output
            forward(input, outputs[FINAL_OUTPUT_INDEX]);
            outputs[ITERATION_OUTPUT_INDEX] = null;
            outputs[ITERATION_CONVERGENCE_OUTPUT_INDEX] = null;
            this.setState(State.FINISHED);
        } else {
            outputs[FINAL_OUTPUT_INDEX] = null;
            forward(input, outputs[ITERATION_OUTPUT_INDEX]);
            // We do not use forward(...) because we might not be able to consume the input ChannelExecutor twice.
            outputs[ITERATION_CONVERGENCE_OUTPUT_INDEX].acceptCollection(convergenceCollection);
            this.setState(State.RUNNING);
        }
    }

    /**
     * Provides the content of the {@code channelExecutor} as a {@link Collection}.
     */
    private static <T> Collection<T> getAsCollection(ChannelExecutor channelExecutor) {
        if (channelExecutor.canProvideCollection()) {
            return channelExecutor.provideCollection();
        } else {
            return channelExecutor.<T>provideStream().collect(Collectors.toList());
        }
    }

    private static void forward(ChannelExecutor source, ChannelExecutor target) {
        if (source.canProvideCollection()) {
            target.acceptCollection(source.provideCollection());
        } else {
            target.acceptStream(source.provideStream());
        }
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaLoopOperator<>(this.getInputType(), this.getConvergenceType(),
                this.getCriterionDescriptor().getJavaImplementation());
    }
}
