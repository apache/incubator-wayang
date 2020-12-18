package org.apache.incubator.wayang.basic.operators;

import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.incubator.wayang.core.optimizer.cardinality.FixedSizeCardinalityEstimator;
import org.apache.incubator.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.incubator.wayang.core.types.DataSetType;

import java.util.Optional;

/**
 * This operator groups the elements of a data set into a single data quantum.
 */
public class GlobalMaterializedGroupOperator<Type> extends UnaryToUnaryOperator<Type, Iterable<Type>> {

    /**
     * Creates a new instance.
     *
     * @param typeClass the class of data quanta being grouped
     */
    public GlobalMaterializedGroupOperator(Class<Type> typeClass) {
        this(DataSetType.createDefault(typeClass), DataSetType.createGrouped(typeClass));
    }

    /**
     * Creates a new instance.
     *
     * @param inputType  the input {@link DataSetType} of the new instance
     * @param outputType the output {@link DataSetType} of the new instance
     */
    public GlobalMaterializedGroupOperator(DataSetType<Type> inputType, DataSetType<Iterable<Type>> outputType) {
        super(inputType, outputType, false);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public GlobalMaterializedGroupOperator(GlobalMaterializedGroupOperator<Type> that) {
        super(that);
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        assert outputIndex == 0;
        return Optional.of(new FixedSizeCardinalityEstimator(1));
    }
}
