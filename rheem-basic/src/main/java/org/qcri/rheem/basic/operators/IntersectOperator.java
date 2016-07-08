package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.BinaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;


/**
 * This operator returns the set intersection of elements of input datasets.
 */
public class IntersectOperator<Type> extends BinaryToUnaryOperator<Type, Type, Type> {

    public IntersectOperator(Class<Type> typeClass) {
        this(DataSetType.createDefault(typeClass));
    }

    public IntersectOperator(DataSetType<Type> dataSetType) {
        super(dataSetType, dataSetType, dataSetType, false);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public IntersectOperator(IntersectOperator<Type> that) {
        super(that);
    }

    /**
     * Provides the type of input and output datasets.
     *
     * @return the {@link DataSetType}
     */
    public DataSetType<Type> getType() {
        return this.getInputType0();
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        // The current idea: We assume that one side is duplicate-free and included in the other.
        // TODO: Find a better estimator.
        return Optional.of(new DefaultCardinalityEstimator(
                .5d, 2, this.isSupportingBroadcastInputs(),
                inputCards -> Math.min(inputCards[0], inputCards[1])
        ));
    }
}
