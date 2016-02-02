package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.optimizer.costs.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.costs.FixedSizeCardinalityEstimator;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Map;
import java.util.Optional;


/**
 * This operator returns the count of elements in this stream.
 */
public class CountOperator<Type> extends UnaryToUnaryOperator<Type, Long> {


    /**
     * Creates a new instance.
     *
     * @param type type of the stream elements
     */
    public CountOperator(DataSetType<Type> type) {
        super(type, DataSetType.createDefault(Long.class), null);
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(
            final int outputIndex,
            final Map<OutputSlot<?>, CardinalityEstimate> cache) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new FixedSizeCardinalityEstimator(1, this.getOutput(outputIndex), cache));
    }
}
