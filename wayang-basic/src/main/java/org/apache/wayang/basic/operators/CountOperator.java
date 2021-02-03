package org.apache.wayang.basic.operators;

import org.apache.commons.lang3.Validate;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.FixedSizeCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

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
        super(type, DataSetType.createDefault(Long.class), false);
    }

    /**
     * Creates a new instance.
     *
     * @param typeClass type of the stream elements
     */
    public CountOperator(Class<Type> typeClass) {
        this(DataSetType.createDefault(typeClass));
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public CountOperator(CountOperator<Type> that) {
        super(that);
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new FixedSizeCardinalityEstimator(1));
    }
}
