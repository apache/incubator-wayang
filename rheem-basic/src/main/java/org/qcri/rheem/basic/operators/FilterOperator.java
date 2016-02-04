package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.Predicate;


/**
 * This operator returns a new dataset after filtering by applying predicate.
 */
public class FilterOperator<Type> extends UnaryToUnaryOperator<Type, Type> {

    /**
     * Function that this operator applies to the input elements.
     */
    protected final Predicate<Type> predicate;

    /**
     * Creates a new instance.
     *
     * @param type type of the dataunit elements
     */
    public FilterOperator(DataSetType<Type> type, Predicate<Type> predicate) {

        super(type, type, null);
        this.predicate = predicate;
    }

    public Predicate<Type> getFunctionDescriptor() {
        return predicate;
    }

    @Override
    public Optional<org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator> getCardinalityEstimator(
            final int outputIndex,
            final Map<OutputSlot<?>, CardinalityEstimate> cache) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new FilterOperator.CardinalityEstimator(this.getOutput(outputIndex), cache));
    }

    /**
     * Custom {@link org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator} for {@link FilterOperator}s.
     */
    private class CardinalityEstimator extends org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator.WithCache {

        public static final double DEFAULT_SELECTIVITY_CORRECTNESS = 0.9;

        public CardinalityEstimator(OutputSlot<?> targetOutput, Map<OutputSlot<?>, CardinalityEstimate> estimateCache) {
            super(targetOutput, estimateCache);
        }

        @Override
        public CardinalityEstimate calculateEstimate(RheemContext rheemContext, CardinalityEstimate... inputEstimates) {
            Validate.isTrue(inputEstimates.length == FilterOperator.this.getNumInputs());
            final CardinalityEstimate inputEstimate = inputEstimates[0];

            final OptionalDouble selectivity = rheemContext.getCardinalityEstimatorManager()
                    .getSelectivity(FilterOperator.this.predicate.getClass());
            if (selectivity.isPresent()) {
                return new CardinalityEstimate(
                        (long) (inputEstimate.getLowerEstimate() * selectivity.getAsDouble()),
                        (long) (inputEstimate.getUpperEstimate() * selectivity.getAsDouble()),
                        inputEstimate.getCorrectnessProbability() * DEFAULT_SELECTIVITY_CORRECTNESS
                );
            } else {
                return new CardinalityEstimate(
                        0l,
                        inputEstimate.getUpperEstimate(),
                        inputEstimate.getCorrectnessProbability()
                );
            }
        }
    }
}
