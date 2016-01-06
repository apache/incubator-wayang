package org.qcri.rheem.core.plan.test;

import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.Sink;

/**
 * Dummy sink for testing purposes.
 */
public class TestSink implements Sink {

    private final InputSlot[] inputSlots = new InputSlot[]{new InputSlot("input", this)};

    @Override
    public InputSlot[] getAllInputs() {
        return this.inputSlots;
    }
}
