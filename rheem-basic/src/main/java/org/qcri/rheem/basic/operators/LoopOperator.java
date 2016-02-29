package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataSetType;

import java.util.*;

/**
 * This operator has three inputs and three outputs.
 */
public class LoopOperator<InputType, ConvergenceType> extends OperatorBase implements ActualOperator, LoopHeadOperator {

    public enum State {
        NOT_STARTED, RUNNING, FINISHED
    }

    public static final int INITIAL_INPUT_INDEX = 0;
    public static final int CONVERGENCE_INPUT_INDEX = 1;
    public static final int ITERATION_INPUT_INDEX = 2;

    public static final int ITERATION_OUTPUT_INDEX = 0;
    public static final int CONVERGENCE_OUTPUT_INDEX= 1;
    public static final int FINAL_OUTPUT_INDEX= 2;

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

    // TODO: Add convenience constructors as in the other operators.

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
        this.inputSlots[INITIAL_INPUT_INDEX] = new InputSlot<>("initialInput", this, inputType);
        this.inputSlots[CONVERGENCE_INPUT_INDEX] = new InputSlot<>("convergenceInput", this, convergenceType);
        this.inputSlots[ITERATION_INPUT_INDEX] = new InputSlot<>("iterationInput", this, inputType);

        this.outputSlots[ITERATION_OUTPUT_INDEX] = new OutputSlot<>("iterationOutput", this, inputType);
        this.outputSlots[CONVERGENCE_OUTPUT_INDEX] = new OutputSlot<>("convergenceOutput", this, convergenceType);
        this.outputSlots[FINAL_OUTPUT_INDEX] = new OutputSlot<>("output", this, inputType);
        this.state = State.NOT_STARTED;
    }


    public DataSetType<InputType> getInputType() {
        return ((InputSlot<InputType>) this.getInput(INITIAL_INPUT_INDEX)).getType();
    }

    public DataSetType<ConvergenceType> getConvergenceType() {
        return ((InputSlot<ConvergenceType>) this.getInput(CONVERGENCE_INPUT_INDEX)).getType();
    }

    public void initialize(Operator initOperator, int initOpOutputIndex) {
        initOperator.connectTo(initOpOutputIndex, this, INITIAL_INPUT_INDEX);
    }

    public void initialize(Operator initOperator) {
        this.initialize(initOperator, 0);
    }

    public void beginIteration(Operator beginOperator, int beginInputIndex, Operator convergeOperator,
                               int convergeInputIndex) {
        this.connectTo(ITERATION_OUTPUT_INDEX, beginOperator, beginInputIndex);
        this.connectTo(CONVERGENCE_OUTPUT_INDEX, convergeOperator, convergeInputIndex);
    }

    public void beginIteration(Operator beginOperator, Operator convergeOperator) {
        this.beginIteration(beginOperator, 0, convergeOperator, 0);
    }

    public void endIteration(Operator endOperator, int endOpOutputIndex, Operator convergeOperator,
                             int convergeOutputIndex) {
        endOperator.connectTo(endOpOutputIndex, this, ITERATION_INPUT_INDEX);
        convergeOperator.connectTo(convergeOutputIndex, this, CONVERGENCE_INPUT_INDEX);
    }

    public void endIteration(Operator endOperator, Operator convergeOperator) {
        this.endIteration(endOperator, 0, convergeOperator, 0);
    }

    public void outputConnectTo(Operator outputOperator, int thatInputIndex) {
        this.connectTo(FINAL_OUTPUT_INDEX, outputOperator, thatInputIndex);
    }

    public void outputConnectTo(Operator outputOperator) {
        this.outputConnectTo(outputOperator, 0);
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

    @Override
    public Collection<OutputSlot<?>> getLoopBodyOutputs() {
        return Arrays.asList(this.getOutput(ITERATION_OUTPUT_INDEX), this.getOutput(CONVERGENCE_OUTPUT_INDEX));
    }

    @Override
    public Collection<OutputSlot<?>> getFinalLoopOutputs() {
        return Collections.singletonList(this.getOutput(FINAL_OUTPUT_INDEX));
    }

    @Override
    public Collection<InputSlot<?>> getLoopBodyInputs() {
        return Arrays.asList(this.getInput(ITERATION_INPUT_INDEX), this.getInput(CONVERGENCE_INPUT_INDEX));
    }

    @Override
    public Collection<InputSlot<?>> getLoopInitializationInputs() {
        return Collections.singletonList(this.getInput(INITIAL_INPUT_INDEX));
    }
}