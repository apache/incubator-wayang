package org.qcri.rheem.core.plan.test;

import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.Sink;

/**
 * Another dummy sink for testing purposes.
 */
public class TestSink2<T> implements Sink {

    private final InputSlot[] inputSlots;

    public TestSink2(Class<T> inputType) {
        this.inputSlots =  new InputSlot[]{new InputSlot<>("input", this, inputType)};
    }

    @Override
    public InputSlot[] getAllInputs() {
        return this.inputSlots;
    }

    public Class<T> getType() {
        return this.inputSlots[0].getType();
    }
}