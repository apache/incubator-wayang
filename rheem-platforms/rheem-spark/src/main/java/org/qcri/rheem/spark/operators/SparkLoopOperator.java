package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.operators.LoopOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.Collection;
import java.util.Optional;

/**
 * Spark implementation of the {@link LoopOperator}.
 */
public class SparkLoopOperator<InputType, ConvergenceType>
        extends LoopOperator<InputType, ConvergenceType>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     */
    public SparkLoopOperator(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType,
                            PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate) {
        super(inputType, convergenceType, criterionPredicate);
    }

    public SparkLoopOperator(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType,
                            PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor) {
        super(inputType, convergenceType, criterionDescriptor);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler,
                         SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Function<Collection<ConvergenceType>, Boolean> stoppingCondition =
                compiler.compile(this.criterionDescriptor, this, inputs);
        boolean endloop = false;

        final Collection<ConvergenceType> convergenceCollection;
        final JavaRDD<ConvergenceType> convergenceRDD;
        final ChannelExecutor input;
        switch (this.getState()) {
            case NOT_STARTED:
                assert inputs[INITIAL_INPUT_INDEX] != null;
                assert inputs[INITIAL_CONVERGENCE_INPUT_INDEX] != null;

                input = inputs[INITIAL_INPUT_INDEX];
                convergenceRDD = inputs[INITIAL_CONVERGENCE_INPUT_INDEX].provideRdd();
                break;
            case RUNNING:
                assert inputs[ITERATION_INPUT_INDEX] != null;
                assert inputs[ITERATION_CONVERGENCE_INPUT_INDEX] != null;

                convergenceRDD = (JavaRDD<ConvergenceType>) inputs[ITERATION_CONVERGENCE_INPUT_INDEX].provideRdd().cache();
                convergenceCollection = convergenceRDD.collect();
                try {
                    endloop = stoppingCondition.call(convergenceCollection);
                } catch (Exception e) {
                    throw new RheemException(
                            String.format("Could not evaluate stopping condition for %s.", this),
                            e
                    );
                }
                input = inputs[ITERATION_INPUT_INDEX];
                break;
            default:
                throw new IllegalStateException(String.format("%s is finished, yet executed.", this));

        }

        if (endloop) {
            // final loop output
            outputs[FINAL_OUTPUT_INDEX].acceptRdd(input.provideRdd());
            outputs[ITERATION_OUTPUT_INDEX] = null;
            outputs[ITERATION_CONVERGENCE_OUTPUT_INDEX] = null;
            this.setState(State.FINISHED);
        } else {
            outputs[FINAL_OUTPUT_INDEX] = null;
            outputs[ITERATION_OUTPUT_INDEX].acceptRdd(input.provideRdd());
            // We do not use forward(...) because we might not be able to consume the input JavaChannelInstance twice.
            outputs[ITERATION_CONVERGENCE_OUTPUT_INDEX].acceptRdd(convergenceRDD);
            this.setState(State.RUNNING);
        }

    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkLoopOperator<>(this.getInputType(), this.getConvergenceType(),
                this.getCriterionDescriptor().getJavaImplementation());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(Configuration configuration) {
        // NB: Not actually measured, adapted from the SparkCollectionSource.
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(4, 3, .8d, CardinalityEstimate.EMPTY_ESTIMATE,
                        (inputCards, outputCards) -> 5000 * inputCards[0] + 6272516800L),
                new DefaultLoadEstimator(4, 3, .9d, CardinalityEstimate.EMPTY_ESTIMATE,
                        (inputCards, outputCards) -> 10000),
                new DefaultLoadEstimator(4, 3, .9d, CardinalityEstimate.EMPTY_ESTIMATE,
                        (inputCards, outputCards) -> 0),
                new DefaultLoadEstimator(4, 3, .9d, CardinalityEstimate.EMPTY_ESTIMATE,
                        (inputCards, outputCards) -> Math.round(4.5d * inputCards[0] + 43000)),
                0.08d,
                1500
        );
        return Optional.of(mainEstimator);
    }
}
