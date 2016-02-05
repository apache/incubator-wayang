package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.BinaryToUnaryOperator;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Map;
import java.util.Optional;

/**
 * This {@link Operator} creates the union (bag semantics) of two .
 */
public class CoalesceOperator<Type> extends BinaryToUnaryOperator<Type, Type, Type> {
    /**
     * Creates a new instance.
     *
     * @param type      the type of the datasets to be coalesced
     */
    public CoalesceOperator(DataSetType<Type> type) {
        super(type, type, type);
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(
            final int outputIndex,
            final Map<OutputSlot<?>, CardinalityEstimate> cache) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(
                1d,
                2,
                inputCards -> inputCards[0] + inputCards[1],
                this.getOutput(outputIndex),
                cache));
    }

    public OutputSlot<?> getOutput() {
        return this.getOutput(0);
    }
}
