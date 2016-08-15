package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.ElementaryOperator;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * This source takes as input a Java {@link java.util.Collection}.
 */
public class CollectionSource<T> extends UnarySource<T> implements ElementaryOperator {

    protected final Collection<T> collection;

    public CollectionSource(Collection<T> collection, Class<T> typeClass) {
        this(collection, DataSetType.createDefault(typeClass));
    }

    public CollectionSource(Collection<T> collection, DataSetType<T> type) {
        super(type);
        this.collection = collection;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public CollectionSource(CollectionSource that) {
        super(that);
        this.collection = that.getCollection();
    }

    public Collection<T> getCollection() {
        return this.collection;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(1d, this.getNumInputs(), this.isSupportingBroadcastInputs(),
                inputCards -> this.collection.size()));
    }

    /**
     * Creates a new instance without any data quanta.
     */
    public static <T> CollectionSource<T> empty(Class<T> typeClass) {
        final CollectionSource<T> instance = new CollectionSource<>(Collections.emptyList(), typeClass);
        instance.setName("{}");
        return instance;
    }

    /**
     * Creates a new instance without any data quanta.
     */
    public static <T> CollectionSource<T> singleton(T value, Class<T> typeClass) {
        final CollectionSource<T> instance = new CollectionSource<>(Collections.singleton(value), typeClass);
        instance.setName("{" + value + "}");
        return instance;
    }
}
