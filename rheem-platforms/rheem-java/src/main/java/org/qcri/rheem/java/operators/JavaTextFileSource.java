package org.qcri.rheem.java.operators;

import org.qcri.rheem.core.plan.*;
import org.qcri.rheem.core.platform.Platform;

/**
 * This is execution operator corresponds to the {@link TextFileSource}.
 */
public class JavaTextFileSource<T> implements JavaExecutionOperator, Source {

    @Override
    public Platform getPlatform() {
        return null;
    }

    @Override
    public OutputSlot[] getAllOutputs() {
        return new OutputSlot[0];
    }
}
