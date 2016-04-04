package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.DoWhileOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Java implementation of the {@link DoWhileOperator}.
 */
public class JavaDoWhileOperator<InputType, ConvergenceType>
        extends DoWhileOperator<InputType, ConvergenceType>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     */
    public JavaDoWhileOperator(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType,
                               PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate) {
        super(inputType, convergenceType, criterionPredicate);
    }

    public JavaDoWhileOperator(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType,
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

                input = inputs[INITIAL_INPUT_INDEX];
                break;
            case RUNNING:
                assert inputs[ITERATION_INPUT_INDEX] != null;
                assert inputs[CONVERGENCE_INPUT_INDEX] != null;

                convergenceCollection = getAsCollection(inputs[CONVERGENCE_INPUT_INDEX]);
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
            this.setState(State.FINISHED);
        } else {
            outputs[FINAL_OUTPUT_INDEX] = null;
            forward(input, outputs[ITERATION_OUTPUT_INDEX]);
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
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(Configuration configuration) {
        // NB: Not actually measured.
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(3, 2, .8d, CardinalityEstimate.EMPTY_ESTIMATE,
                        (inputCards, outputCards) -> 500 * inputCards[CONVERGENCE_INPUT_INDEX] + 810000),
                new DefaultLoadEstimator(3, 2, 0, CardinalityEstimate.EMPTY_ESTIMATE, (inputCards, outputCards) -> 0)
        );
        return Optional.of(mainEstimator);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaDoWhileOperator<>(this.getInputType(), this.getConvergenceType(),
                this.getCriterionDescriptor().getJavaImplementation());
    }
}
