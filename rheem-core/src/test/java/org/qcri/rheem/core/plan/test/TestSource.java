package org.qcri.rheem.core.plan.test;

import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.Source;

/**
 * Dummy sink for testing purposes.
 */
public class TestSource implements Source {

    private final OutputSlot[] outputSlots = new OutputSlot[]{new OutputSlot("output", this)};

    @Override
    public OutputSlot[] getAllOutputs() {
        return this.outputSlots;
    }
}
