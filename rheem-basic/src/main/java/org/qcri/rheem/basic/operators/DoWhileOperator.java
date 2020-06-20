package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.SwitchForwardCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.ElementaryOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OperatorBase;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.ReflectionUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * This operator has three inputs and two outputs.
 */
public class DoWhileOperator<InputType, ConvergenceType> extends OperatorBase implements ElementaryOperator, LoopHeadOperator {

    public static final int INITIAL_INPUT_INDEX = 0;
    public static final int ITERATION_INPUT_INDEX = 1;
    public static final int CONVERGENCE_INPUT_INDEX = 2;

    public static final int ITERATION_OUTPUT_INDEX = 0;
    public static final int FINAL_OUTPUT_INDEX = 1;

    /**
     * Function that this operator applies to the input elements.
     */
    protected final PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor;

    private Integer numExpectedIterations = 0;

    private State state;

    @Override
    public State getState() {
        return state;
    }

    @Override
    public void setState(State state) {
        this.state = state;
    }

    public DoWhileOperator(Class<InputType> inputTypeClass,
                           Class<ConvergenceType> convergenceTypeClass,
                           PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate,
                           Integer numExpectedIterations) {
        this(DataSetType.createDefault(inputTypeClass),
                DataSetType.createDefault(convergenceTypeClass),
                criterionPredicate,
                numExpectedIterations
        );
    }

    public DoWhileOperator(DataSetType<InputType> inputType,
                           DataSetType<ConvergenceType> convergenceType,
                           PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate,
                           Integer numExpectedIterations) {
        this(inputType,
                convergenceType,
                new PredicateDescriptor<>(criterionPredicate, ReflectionUtils.specify(Collection.class)),
                numExpectedIterations
        );
    }

    /**
     * Creates a new instance.
     */
    public DoWhileOperator(DataSetType<InputType> inputType,
                           DataSetType<ConvergenceType> convergenceType,
                           PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor,
                           Integer numExpectedIterations) {
        super(3, 2, true);
        this.criterionDescriptor = criterionDescriptor;
        this.numExpectedIterations = numExpectedIterations;
        this.state = State.NOT_STARTED;
        this.initializeSlots(inputType, convergenceType);
    }

    /**
     * Creates a copy of the given {@link LoopOperator}.
     *
     * @param that should be copied
     */
    public DoWhileOperator(DoWhileOperator<InputType, ConvergenceType> that) {
        super(that);
        this.criterionDescriptor = that.getCriterionDescriptor();
        this.numExpectedIterations = that.getNumExpectedIterations();
        this.state = that.getState();
        this.initializeSlots(that.getInputType(), that.getConvergenceType());
    }

    private void initializeSlots(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType) {
        this.inputSlots[INITIAL_INPUT_INDEX] = new InputSlot<>("initIn", this, inputType);
        this.inputSlots[ITERATION_INPUT_INDEX] = new InputSlot<>("iterIn", this, inputType);
        this.inputSlots[CONVERGENCE_INPUT_INDEX] = new InputSlot<>("convIn", this, convergenceType);

        this.outputSlots[ITERATION_OUTPUT_INDEX] = new OutputSlot<>("iterOut", this, inputType);
        this.outputSlots[FINAL_OUTPUT_INDEX] = new OutputSlot<>("finOut", this, inputType);
    }

    @SuppressWarnings("unchecked")
    public DataSetType<InputType> getInputType() {
        return ((InputSlot<InputType>) this.getInput(INITIAL_INPUT_INDEX)).getType();
    }

    @SuppressWarnings("unchecked")
    public DataSetType<ConvergenceType> getConvergenceType() {
        return ((InputSlot<ConvergenceType>) this.getInput(CONVERGENCE_INPUT_INDEX)).getType();
    }

    public void initialize(Operator initOperator, int initOpOutputIndex) {
        initOperator.connectTo(initOpOutputIndex, this, INITIAL_INPUT_INDEX);
    }

    public void beginIteration(Operator beginOperator, int beginInputIndex) {
        this.connectTo(ITERATION_OUTPUT_INDEX, beginOperator, beginInputIndex);
    }

    public void endIteration(Operator endOperator, int endOpOutputIndex, Operator convergeOperator,
                             int convergeOutputIndex) {
        endOperator.connectTo(endOpOutputIndex, this, ITERATION_INPUT_INDEX);
        convergeOperator.connectTo(convergeOutputIndex, this, CONVERGENCE_INPUT_INDEX);
    }

    public void outputConnectTo(Operator outputOperator, int thatInputIndex) {
        this.connectTo(FINAL_OUTPUT_INDEX, outputOperator, thatInputIndex);
    }

    public PredicateDescriptor<Collection<ConvergenceType>> getCriterionDescriptor() {
        return this.criterionDescriptor;
    }

    @Override
    public Collection<OutputSlot<?>> getForwards(InputSlot<?> input) {
        assert this.isOwnerOf(input);
        switch (input.getIndex()) {
            case INITIAL_INPUT_INDEX:
            case ITERATION_INPUT_INDEX:
                return Arrays.asList(this.getOutput(ITERATION_OUTPUT_INDEX), this.getOutput(FINAL_OUTPUT_INDEX));
            default:
                return super.getForwards(input);
        }
    }

    @Override
    public boolean isReading(InputSlot<?> input) {
        assert this.isOwnerOf(input);
        switch (input.getIndex()) {
            case CONVERGENCE_INPUT_INDEX:
            case INITIAL_INPUT_INDEX:
            case ITERATION_INPUT_INDEX:
                return true;
            default:
                return super.isReading(input);
        }
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex, Configuration configuration) {
        switch (outputIndex) {
            case ITERATION_OUTPUT_INDEX:
            case FINAL_OUTPUT_INDEX:
                return Optional.of(new SwitchForwardCardinalityEstimator(INITIAL_INPUT_INDEX, ITERATION_INPUT_INDEX));
            default:
                throw new IllegalArgumentException("Illegal output index " + outputIndex + ".");
        }
    }

    @Override
    public Collection<OutputSlot<?>> getLoopBodyOutputs() {
        return Collections.singletonList(this.getOutput(ITERATION_OUTPUT_INDEX));
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

    @Override
    public Collection<InputSlot<?>> getConditionInputSlots() {
        return Collections.singletonList(this.getInput(CONVERGENCE_INPUT_INDEX));
    }

    @Override
    public Collection<OutputSlot<?>> getConditionOutputSlots() {
        return Collections.emptyList();
    }

    public void setNumExpectedIterations(int numExpectedIterations) {
        this.numExpectedIterations = numExpectedIterations;
    }

    @Override
    public int getNumExpectedIterations() {
        return this.numExpectedIterations;
    }

}