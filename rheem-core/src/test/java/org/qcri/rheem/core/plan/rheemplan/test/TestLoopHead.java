package org.qcri.rheem.core.plan.rheemplan.test;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.*;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * {@link LoopHeadOperator} implementation for test purposes.
 */
public class TestLoopHead<T> extends OperatorBase implements LoopHeadOperator {

    private int numExpectedIterations;

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

    @Override
    public int getNumExpectedIterations() {
        return this.numExpectedIterations;
    }

    public void setNumExpectedIterations(int numExpectedIterations) {
        this.numExpectedIterations = numExpectedIterations;
    }

    @Override
    public CardinalityPusher getCardinalityPusher(Configuration configuration) {
        return new DefaultCardinalityPusher(this,
                Slot.toIndices(this.getLoopBodyInputs()),
                Slot.toIndices(this.getLoopBodyOutputs()),
                configuration.getCardinalityEstimatorProvider());
    }

    @Override
    public CardinalityPusher getInitializationPusher(Configuration configuration) {
        return new DefaultCardinalityPusher(this,
                Slot.toIndices(this.getLoopInitializationInputs()),
                Slot.toIndices(this.getLoopBodyOutputs()),
                configuration.getCardinalityEstimatorProvider());
    }

    @Override
    public CardinalityPusher getFinalizationPusher(Configuration configuration) {
        return new DefaultCardinalityPusher(this,
                Slot.toIndices(this.getLoopBodyInputs()),
                Slot.toIndices(this.getFinalLoopOutputs()),
                configuration.getCardinalityEstimatorProvider());
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(int outputIndex, Configuration configuration) {
        switch (outputIndex) {
            case 0:
            case 1:
                return Optional.of(new SwitchForwardCardinalityEstimator(0, 1));
            default:
                throw new IllegalArgumentException("Illegal output index " + outputIndex + ".");
        }
    }
}
