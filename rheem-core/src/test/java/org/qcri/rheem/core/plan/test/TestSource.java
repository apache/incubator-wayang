package org.qcri.rheem.core.plan.test;

import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.Source;
import org.qcri.rheem.core.types.DataSet;

/**
 * Dummy source for testing purposes.
 */
public class TestSource<T> implements Source {

    private final OutputSlot[] outputSlots;

    public TestSource(DataSet outputType) {
        this.outputSlots = new OutputSlot[]{new OutputSlot<>("output", this, outputType)};
    }

    @Override
    public OutputSlot[] getAllOutputs() {
        return this.outputSlots;
    }
}
