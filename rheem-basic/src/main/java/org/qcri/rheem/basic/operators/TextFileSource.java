package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.Source;

/**
 * This source reads a text file and outputs the lines as data units.
 */
public class TextFileSource implements Source {

    private final OutputSlot<String> outputSlot = new OutputSlot<>("lines", this, String.class);

    private final OutputSlot<String>[] outputSlots = new OutputSlot[] { outputSlot };

    private final String inputUrl;

    public TextFileSource(String inputUrl) {
        this.inputUrl = inputUrl;
    }

    @Override
    public OutputSlot[] getAllOutputs() {
        return outputSlots;
    }

    public String getInputUrl() {
        return inputUrl;
    }
}
