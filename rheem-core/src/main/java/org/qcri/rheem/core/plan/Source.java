package org.qcri.rheem.core.plan;

/**
 * A source is an operator that has no inputs.
 */
public interface Source extends Operator {

    InputSlot[] INPUT_SLOTS = new InputSlot[0];

    @Override
    default InputSlot[] getAllInputs() {
        return INPUT_SLOTS;
    }

}
