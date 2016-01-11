package org.qcri.rheem.core.plan.test;

import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.Sink;
import org.qcri.rheem.core.types.DataSet;
import org.qcri.rheem.core.types.FlatDataSet;

/**
 * Another dummy sink for testing purposes.
 */
public class TestSink2<T> implements Sink {

    private final InputSlot[] inputSlots;

    public TestSink2(FlatDataSet inputType) {
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