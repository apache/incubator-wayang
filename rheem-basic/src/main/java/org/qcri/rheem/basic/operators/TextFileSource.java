package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.plan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;

/**
 * This source reads a text file and outputs the lines as data units.
 */
public class TextFileSource extends UnarySource {

    private final String inputUrl;

    public TextFileSource(String inputUrl) {
        super(DataSetType.createDefault(String.class), null);
        this.inputUrl = inputUrl;
    }

    public String getInputUrl() {
        return inputUrl;
    }
}
