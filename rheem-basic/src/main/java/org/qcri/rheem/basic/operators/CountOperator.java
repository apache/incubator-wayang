package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.FixedSizeCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

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
        super(type, DataSetType.createDefault(Long.class), false, null);
    }

    /**
     * Creates a new instance.
     *
     * @param typeClass type of the stream elements
     */
    public CountOperator(Class<Type> typeClass) {
        this(DataSetType.createDefault(typeClass));
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new FixedSizeCardinalityEstimator(1));
    }
}
