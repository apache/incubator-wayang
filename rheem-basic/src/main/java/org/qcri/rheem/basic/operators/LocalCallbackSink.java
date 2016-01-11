package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.Sink;
import org.qcri.rheem.core.types.DataSet;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * This sink executes a callback on each received data unit into a Java {@link Collection}.
 */
public class LocalCallbackSink<T> implements Sink {

    private final InputSlot<T> inputSlot;

    private final InputSlot<T>[] inputSlots;

    protected final Consumer<T> callback;

    public static <T> LocalCallbackSink<T> createCollectingSink(Collection<T> collector, DataSet type) {
        return new LocalCallbackSink<>(dataUnit -> collector.add(dataUnit), type);
    }


    /**
     * Creates a new instance.
     *
     * @param callback callback that is executed locally for each incoming data unit
     * @param type     type of the incoming elements
     */
    public LocalCallbackSink(Consumer<T> callback, DataSet type) {
        this.inputSlot = new InputSlot<>("elements", this, type);
        this.inputSlots = new InputSlot[]{this.inputSlot};
        this.callback = callback;
    }

    @Override
    public InputSlot<?>[] getAllInputs() {
        return this.inputSlots;
    }

    public InputSlot<T> getInput() {
        return this.inputSlot;
    }

    public Consumer<T> getCallback() {
        return callback;
    }

    public DataSet getType() {
        return this.inputSlot.getType();
    }
}
