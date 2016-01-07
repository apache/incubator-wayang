package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.Source;

/**
 * This sink prints all incoming data units to the {@code stdout}.
 */
public class StdoutSink<T> implements Source {

    private final OutputSlot<T> outputSlot;

    private final OutputSlot<T>[] outputSlots;

    public StdoutSink(Class<T> type) {
        this.outputSlot = new OutputSlot<>("input", this, type);
        this.outputSlots = new OutputSlot[] { outputSlot };
    }

    @Override
    public OutputSlot[] getAllOutputs() {
        return outputSlots;
    }
}
