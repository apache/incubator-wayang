package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

/**
 * This operator has three inputs and three outputs.
 */
public class LoopOperator<InputType, ConvergenceType> extends OperatorBase implements ActualOperator {
    public enum State {
        NOT_STARTED, RUNNING, FINISHED
    }

    /**
     * Function that this operator applies to the input elements.
     */
    protected final PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor;

    private State state;

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public LoopOperator(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType,
                        PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate) {
        this(inputType, convergenceType,
                new PredicateDescriptor<>(criterionPredicate, (BasicDataUnitType)convergenceType.getDataUnitType()));
    }

    /**
     * Creates a new instance.
     */
    public LoopOperator(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType,
                        PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor) {
        super(3, 3, true, null);
        this.criterionDescriptor = criterionDescriptor;
        this.inputSlots[0] = new InputSlot<>("initialInput", this, inputType);
        this.inputSlots[1] = new InputSlot<>("convergenceInput", this, convergenceType);
        this.inputSlots[2] = new InputSlot<>("iterationInput", this, inputType);

        this.outputSlots[0] = new OutputSlot<>("iterationOutput", this, inputType);
        this.outputSlots[1] = new OutputSlot<>("convergenceOutput", this, convergenceType);
        this.outputSlots[2] = new OutputSlot<>("output", this, inputType);
        this.state = State.NOT_STARTED;
    }


    public DataSetType<InputType> getInputType() {
        return ((InputSlot<InputType>) this.getInput(0)).getType();
    }

    public DataSetType<ConvergenceType> getConvergenceType() {
        return ((InputSlot<ConvergenceType>) this.getInput(1)).getType();
    }

    public void initialize(Operator initOperator, int initOpOutputIndex) {
        initOperator.connectTo(initOpOutputIndex, this, 0);
    }

    public void initialize(Operator initOperator) {
        initOperator.connectTo(0, this, 0);
    }

    public void beginIteration(Operator beginOperator, int beginInputIndex, Operator convergeOperator,
                               int convergeInputIndex) {
        this.connectTo(0, beginOperator, beginInputIndex);
        this.connectTo(1, convergeOperator, convergeInputIndex);
    }

    public void beginIteration(Operator beginOperator, Operator convergeOperator) {
        this.connectTo(0, beginOperator, 0);
        this.connectTo(1, convergeOperator, 0);
    }

    public void endIteration(Operator endOperator, int endOpOutputIndex, Operator convergeOperator,
                             int convergeOutputIndex) {
        endOperator.connectTo(endOpOutputIndex, this, 2);
        convergeOperator.connectTo(convergeOutputIndex, this, 1);
    }

    public void endIteration(Operator endOperator, Operator convergeOperator) {
        endOperator.connectTo(0, this, 2);
        convergeOperator.connectTo(0, this, 1);
    }

    public void outputConnectTo(Operator outputOperator, int thatInputIndex) {
        this.connectTo(2, outputOperator, thatInputIndex);
    }

    public void outputConnectTo(Operator outputOperator) {
        this.connectTo(2, outputOperator, 0);
    }

    public PredicateDescriptor<Collection<ConvergenceType>> getCriterionDescriptor() {
        return this.criterionDescriptor;
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, this.isSupportingBroadcastInputs(), inputCards -> inputCards[0]));
    }
}