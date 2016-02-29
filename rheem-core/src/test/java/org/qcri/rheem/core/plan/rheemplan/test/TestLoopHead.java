package org.qcri.rheem.core.plan.rheemplan.test;

import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;

import java.util.Collection;
import java.util.Collections;

/**
 * {@link LoopHeadOperator} implementation for test purposes.
 */
public class TestLoopHead<T> extends OperatorBase implements LoopHeadOperator {

    public TestLoopHead(Class<T> dataQuantumClass) {
        super(2, 2, false, null);

        final DataSetType<T> dataSetType = DataSetType.createDefault(DataUnitType.createBasic(dataQuantumClass));
        this.inputSlots[0] = new InputSlot<>("initialInput", this, dataSetType);
        this.inputSlots[1] = new InputSlot<>("loopInput", this, dataSetType);
        this.outputSlots[0] = new OutputSlot<>("loopOutput", this, dataSetType);
        this.outputSlots[1] = new OutputSlot<>("finalOutput", this, dataSetType);
    }

    @Override
    public Collection<OutputSlot<?>> getLoopBodyOutputs() {
        return Collections.singleton(this.getOutput("loopOutput"));
    }

    @Override
    public Collection<OutputSlot<?>> getFinalLoopOutputs() {
        return Collections.singleton(this.getOutput("finalOutput"));
    }

    @Override
    public Collection<InputSlot<?>> getLoopBodyInputs() {
        return Collections.singleton(this.getInput("loopInput"));
    }

    @Override
    public Collection<InputSlot<?>> getLoopInitializationInputs() {
        return Collections.singleton(this.getInput("initialInput"));
    }
}
