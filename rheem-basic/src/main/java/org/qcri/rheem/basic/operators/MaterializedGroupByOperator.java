package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * This operator collocates the data units in a data set w.r.t. a key function.
 */
public class MaterializedGroupByOperator<Type, Key> extends UnaryToUnaryOperator<Type, Iterator<Type>> {

    protected final TransformationDescriptor keyDescriptor;

    /**
     * Creates a new instance.
     *
     * @param type          type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param keyDescriptor describes how to extract the key from data units
     */
    public MaterializedGroupByOperator(DataSetType<Type> type,
                                       TransformationDescriptor<Type, Key> keyDescriptor) {
        super(type, type, null);
        this.keyDescriptor = keyDescriptor;
    }

    public DataSetType<Type> getType() {
        return this.getInputType();
    }

    public TransformationDescriptor<Type, Key> getKeyDescriptor() {
        return keyDescriptor;
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        // TODO: Come up with a decent way to estimate the "distinctness" of reduction keys.
        return Optional.of(new DefaultCardinalityEstimator(0.5d, 1, inputCards -> (long) (inputCards[0] * 0.1)));
    }

}
