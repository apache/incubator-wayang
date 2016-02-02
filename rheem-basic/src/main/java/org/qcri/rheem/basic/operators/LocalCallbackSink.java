package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.optimizer.costs.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.costs.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.UnarySink;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * This sink executes a callback on each received data unit into a Java {@link Collection}.
 */
public class LocalCallbackSink<T> extends UnarySink<T> {

    protected final Consumer<T> callback;

    public static <T> LocalCallbackSink<T> createCollectingSink(Collection<T> collector, DataSetType<T> type) {
        return new LocalCallbackSink<>(collector::add, type);
    }

    /**
     * Creates a new instance.
     *
     * @param callback callback that is executed locally for each incoming data unit
     * @param type     type of the incoming elements
     */
    public LocalCallbackSink(Consumer<T> callback, DataSetType<T> type) {
        super(type, null);
        this.callback = callback;
    }

    public Consumer<T> getCallback() {
        return callback;
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(int outputIndex) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return super.getCardinalityEstimator(outputIndex);
    }
}
