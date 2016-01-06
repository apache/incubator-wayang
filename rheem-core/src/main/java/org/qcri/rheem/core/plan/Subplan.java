package org.qcri.rheem.core.plan;

/**
 * Created by basti on 01/06/16.
 */
public class Subplan implements Operator {

    @Override
    public InputSlot[] getAllInputs() {
        return new InputSlot[0];
    }

    @Override
    public OutputSlot[] getAllOutputs() {
        return new OutputSlot[0];
    }
}
