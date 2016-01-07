package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.Sink;

/**
 * This sink prints all incoming data units to the {@code stdout}.
 */
public class StdoutSink<T> implements Sink {

    private final InputSlot<T> inputSlot;

    private final InputSlot<T>[] inputSlots;

    public StdoutSink(Class<T> type) {
        this.inputSlot = new InputSlot<>("input", this, type);
        this.inputSlots = new InputSlot[]{inputSlot};
    }

    @Override
    public InputSlot[] getAllInputs() {
        return inputSlots;
    }

    public Class<T> getType() {
        return this.inputSlot.getType();
    }
}
