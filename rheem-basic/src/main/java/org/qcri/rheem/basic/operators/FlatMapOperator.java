package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;

/**
 * A flatmap operator represents semantics as they are known from frameworks, such as Spark and Flink. It pulls each
 * available element from the input slot, applies a function to it, returning zero or more output elements,
 * flattening the result and pushes it to the output slot.
 */
public class FlatMapOperator<InputType, OutputType> extends UnaryToUnaryOperator<InputType, OutputType> {

    /**
     * Function that this operator applies to the input elements.
     */
    protected final FlatMapDescriptor<InputType, OutputType> functionDescriptor;

    /**
     * Creates a new instance.
     */
    public FlatMapOperator(FlatMapDescriptor<InputType, OutputType> functionDescriptor) {
        super(DataSetType.createDefault(functionDescriptor.getInputType()),
                DataSetType.createDefault(functionDescriptor.getOutputType()),
                true);
        this.functionDescriptor = functionDescriptor;
    }

    /**
     * Creates a new instance.
     */
    public FlatMapOperator(FunctionDescriptor.SerializableFunction<InputType, Iterable<OutputType>> function,
                           Class<InputType> inputTypeClass,
                           Class<OutputType> outputTypeClass) {
        this(new FlatMapDescriptor<>(function, inputTypeClass, outputTypeClass));
    }

    /**
     * Creates a new instance.
     */
    public FlatMapOperator(FlatMapDescriptor<InputType, OutputType> functionDescriptor,
                           DataSetType<InputType> inputType,
                           DataSetType<OutputType> outputType) {
        super(inputType, outputType, true);
        this.functionDescriptor = functionDescriptor;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlatMapOperator(FlatMapOperator<InputType, OutputType> that) {
        super(that);
        this.functionDescriptor = that.functionDescriptor;
    }

    public FlatMapDescriptor<InputType, OutputType> getFunctionDescriptor() {
        return this.functionDescriptor;
    }

    @Override
    public Optional<org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new FlatMapOperator.CardinalityEstimator(configuration));
    }

    /**
     * Custom {@link org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator} for {@link FlatMapOperator}s.
     */
    private class CardinalityEstimator implements org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator {

        /**
         * The selectivity of this instance.
         */
        private final ProbabilisticDoubleInterval selectivity;

        private CardinalityEstimator(Configuration configuration) {
            this.selectivity = configuration
                    .getUdfSelectivityProvider()
                    .provideFor(FlatMapOperator.this.functionDescriptor);
        }

        @Override
        public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
            assert FlatMapOperator.this.getNumInputs() == inputEstimates.length;
            final CardinalityEstimate inputEstimate = inputEstimates[0];
            return new CardinalityEstimate(
                    (long) (inputEstimate.getLowerEstimate() * this.selectivity.getLowerEstimate()),
                    (long) (inputEstimate.getUpperEstimate() * this.selectivity.getUpperEstimate()),
                    inputEstimate.getCorrectnessProbability() * this.selectivity.getCorrectnessProbability()
            );
        }
    }
}
