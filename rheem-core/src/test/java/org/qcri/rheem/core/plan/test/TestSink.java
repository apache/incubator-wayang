package org.qcri.rheem.core.plan.test;

import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.Sink;
import org.qcri.rheem.core.types.FlatDataSet;

/**
 * Dummy sink for testing purposes.
 */
public class TestSink<T> implements Sink {

    private final InputSlot[] inputSlots;

    public TestSink(FlatDataSet inputType) {
        this.inputSlots =  new InputSlot[]{new InputSlot<>("input", this, inputType)};
    }

    @Override
    public InputSlot[] getAllInputs() {
        return this.inputSlots;
    }

    public FlatDataSet getType() {
        return (FlatDataSet) this.inputSlots[0].getType();
    }
}
