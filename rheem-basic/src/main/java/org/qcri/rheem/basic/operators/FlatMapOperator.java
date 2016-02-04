package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.Stream;

/**
 * A flatmap operator represents semantics as they are known from frameworks, such as Spark and Flink. It pulls each
 * available element from the input slot, applies a function to it, returning zero or more output elements,
 * flattening the result and pushes it to the output slot.
 */
public class FlatMapOperator<InputType, OutputType> extends UnaryToUnaryOperator<InputType, OutputType> {

    /**
     * Function that this operator applies to the input elements.
     */
    protected final TransformationDescriptor<InputType, Stream<OutputType>> functionDescriptor;

    /**
     * Creates a new instance.
     */
    public FlatMapOperator(DataSetType<InputType> inputType, DataSetType<OutputType> outputType,
                           TransformationDescriptor<InputType, Stream<OutputType>> functionDescriptor) {
        super(inputType, outputType, null);
        this.functionDescriptor = functionDescriptor;
    }

    public TransformationDescriptor<InputType, Stream<OutputType>> getFunctionDescriptor() {
        return functionDescriptor;
    }

    @Override
    public Optional<org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator> getCardinalityEstimator(
            final int outputIndex,
            final Map<OutputSlot<?>, CardinalityEstimate> cache) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new FlatMapOperator.CardinalityEstimator(this.getOutput(outputIndex), cache));
    }

    /**
     * Custom {@link org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator} for {@link FlatMapOperator}s.
     */
    private class CardinalityEstimator extends org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator.WithCache {

        public static final double DEFAULT_SELECTIVITY_CORRECTNESS = 0.9;

        /**
         * We expect selectivities to be between {@value #DEFAULT_SELECTIVITY_DEVIATION} and {@code 1/}{@value #DEFAULT_SELECTIVITY_DEVIATION}.
         */
        public static final double DEFAULT_SELECTIVITY_DEVIATION = 0.01;

        public CardinalityEstimator(OutputSlot<?> targetOutput, Map<OutputSlot<?>, CardinalityEstimate> estimateCache) {
            super(targetOutput, estimateCache);
        }

        @Override
        public CardinalityEstimate calculateEstimate(RheemContext rheemContext, CardinalityEstimate... inputEstimates) {
            Validate.isTrue(FlatMapOperator.this.getNumInputs() == inputEstimates.length);
            final CardinalityEstimate inputEstimate = inputEstimates[0];

            final OptionalDouble selectivity = rheemContext.getCardinalityEstimatorManager()
                    .getSelectivity(FlatMapOperator.this.functionDescriptor);
            if (selectivity.isPresent()) {
                return new CardinalityEstimate(
                        (long) (inputEstimate.getLowerEstimate() * selectivity.getAsDouble()),
                        (long) (inputEstimate.getUpperEstimate() * selectivity.getAsDouble()),
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
