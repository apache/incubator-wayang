package org.qcri.rheem.core.plan;

/**
 * A sink is an operator that has no outputs.
 */
public interface Sink extends Operator {

    OutputSlot[] OUTPUT_SLOTS = new OutputSlot[0];

    @Override
    default OutputSlot[] getAllOutputs() {
        return OUTPUT_SLOTS;
    }

}
