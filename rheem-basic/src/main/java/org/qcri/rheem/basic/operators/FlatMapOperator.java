package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.FlatMapDescriptor;
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
    public FlatMapOperator(DataSetType<InputType> inputType, DataSetType<OutputType> outputType,
                           FlatMapDescriptor<InputType, OutputType> functionDescriptor) {
        super(inputType, outputType, true, null);
        this.functionDescriptor = functionDescriptor;
    }

    public FlatMapDescriptor<InputType, OutputType> getFunctionDescriptor() {
        return this.functionDescriptor;
    }

    @Override
    public Optional<org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator> getCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new FlatMapOperator.CardinalityEstimator());
    }

    /**
     * Custom {@link org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator} for {@link FlatMapOperator}s.
     */
    private class CardinalityEstimator implements org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator {

        public static final double DEFAULT_SELECTIVITY_CORRECTNESS = 0.9;

        /**
         * We expect selectivities to be between {@value #DEFAULT_SELECTIVITY_DEVIATION} and {@code 1/}{@value #DEFAULT_SELECTIVITY_DEVIATION}.
         */
        public static final double DEFAULT_SELECTIVITY_DEVIATION = 0.1;

        @Override
        public CardinalityEstimate estimate(Configuration configuration, CardinalityEstimate... inputEstimates) {
            assert FlatMapOperator.this.getNumInputs() == inputEstimates.length;
            final CardinalityEstimate inputEstimate = inputEstimates[0];

            final Optional<Double> selectivity = configuration.getMultimapSelectivityProvider().optionallyProvideFor(
                    FlatMapOperator.this.functionDescriptor);
            if (selectivity.isPresent()) {
                return new CardinalityEstimate(
                        (long) (inputEstimate.getLowerEstimate() * selectivity.get()),
                        (long) (inputEstimate.getUpperEstimate() * selectivity.get()),
                        inputEstimate.getCorrectnessProbability() * DEFAULT_SELECTIVITY_CORRECTNESS
                );
            } else {
                return new CardinalityEstimate(
                        (long) (inputEstimate.getLowerEstimate() * DEFAULT_SELECTIVITY_DEVIATION),
                        (long) (inputEstimate.getUpperEstimate() / DEFAULT_SELECTIVITY_DEVIATION),
                        inputEstimate.getCorrectnessProbability() * DEFAULT_SELECTIVITY_CORRECTNESS
                );
            }
        }
    }
}
