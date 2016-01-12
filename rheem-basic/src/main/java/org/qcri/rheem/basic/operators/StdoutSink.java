package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.UnarySink;
import org.qcri.rheem.core.types.DataSet;

/**
 * This sink prints all incoming data units to the {@code stdout}.
 */
public class StdoutSink<T> extends UnarySink<T> {

    public StdoutSink(DataSet inputType) {
        super(inputType, null);
    }

    @Override
    public InputSlot[] getAllInputs() {
        return inputSlots;
    }

}
