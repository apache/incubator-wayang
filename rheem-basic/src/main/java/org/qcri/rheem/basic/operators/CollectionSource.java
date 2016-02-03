package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.optimizer.costs.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.costs.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.ActualOperator;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * This source takes as input a Java {@link java.util.Collection}.
 */
public class CollectionSource<T> extends UnarySource<T> implements ActualOperator {

    protected final Collection<T> collection;

    public CollectionSource(Collection<T> collection, DataSetType<T> type) {
        super(type, null);
        this.collection = collection;
    }

    public Collection<T> getCollection() {
        return collection;
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(
            final int outputIndex,
            final Map<OutputSlot<?>, CardinalityEstimate> cache) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(
                1d,
                getNumInputs(),
                inputCards -> this.collection.size(),
                this.getOutput(outputIndex),
                cache));
    }
}
