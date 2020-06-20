package org.qcri.rheem.core.mapping;

import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OperatorBase;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.Slot;
import org.qcri.rheem.core.plan.rheemplan.TopDownPlanVisitor;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Collection;
import java.util.LinkedList;
import java.util.function.Predicate;

/**
 * An operator pattern matches to a class of operator instances.
 */
public class OperatorPattern<T extends Operator> extends OperatorBase {

    /**
     * Identifier for this instance to identify {@link OperatorMatch}es.
     */
    private final String name;

    /**
     * {@link Operator} type matched by this instance.
     */
    private final Class<?> operatorClass;

    /**
     * Whether subclasses of {@link #operatorClass} also match.
     */
    private final boolean isMatchSubclasses;

    /**
     * Whether broadcast {@link InputSlot}s are allowed.
     */
    private final boolean isAllowBroadcasts;

    /**
     * Additional predicates to test in order to establish a match.
     */
    private final Collection<Predicate<T>> additionalTests = new LinkedList<>();

    /**
     * Creates a new instance.
     *
     * @param name              used to identify the new instance (e.g., in {@link SubplanMatch}es)
     * @param exampleOperator   serves as template of the {@link Operator}s to match; use {@link DataSetType#none()} to
     *                          state that the {@link DataSetType} of a certain {@link Slot} are not to be matched
     * @param isMatchSubclasses whether to match subclasses of the {@code exampleOperator}
     */
    public OperatorPattern(String name,
                           T exampleOperator,
                           boolean isMatchSubclasses) {

        super(exampleOperator.getNumInputs(), exampleOperator.getNumOutputs(),
                exampleOperator.isSupportingBroadcastInputs());

        this.name = name;
        InputSlot.mock(exampleOperator, this);
        OutputSlot.mock(exampleOperator, this);

        this.operatorClass = exampleOperator.getClass();
        this.isAllowBroadcasts = exampleOperator.isSupportingBroadcastInputs();
        this.isMatchSubclasses = isMatchSubclasses;
    }

    /**
     * Test whether this pattern matches a given operator.
     *
     * @param operator the operator to match or {@code null}, which represents the absence of an operator to match
     * @return whether the operator matches
     */
    @SuppressWarnings("unchecked")
    public OperatorMatch match(Operator operator) {
        if (operator == null) return null;

        // Only match by the class so far.
        if (this.matchOperatorClass(operator) && this.matchSlots(operator) && this.matchAdditionalTests((T) operator)) {
            this.checkSanity(operator);
            return new OperatorMatch(this, operator);
        }

        return null;
    }

    /**
     * Checks whether the {@link Operator} {@link Class} that of the given {@link Operator}.
     *
     * @param operator that should be matched with
     * @return whether this instance and the {@code operator} match
     */
    private boolean matchOperatorClass(Operator operator) {
        return this.isMatchSubclasses ?
                this.operatorClass.isAssignableFrom(operator.getClass()) :
                this.operatorClass.equals(operator.getClass());
    }

    /**
     * Checks whether the {@link Operator} {@link Class} that of the given {@link Operator}. TODO
     *
     * @param operator that should be matched with
     * @return whether this instance and the {@code operator} match
     */
    private boolean matchSlots(Operator operator) {
        // Check whether the InputSlots match.
        int inputIndex;
        for (inputIndex = 0; inputIndex < this.getNumInputs(); inputIndex++) {
            InputSlot<?> slotPattern = this.getInput(inputIndex);
            final InputSlot<?> testSlot = operator.getInput(slotPattern.getIndex());
            if (!this.matchSlot(slotPattern, testSlot)) {
                return false;
            }
        }
        // Take special care for broadcasts.
        for (; inputIndex < operator.getNumInputs(); inputIndex++) {
            if (operator.getInput(inputIndex).isBroadcast() && !this.isAllowBroadcasts) return false;
        }

        // Check whether the OutputSlots match.
        for (int outputIndex = 0; outputIndex < this.getNumOutputs(); outputIndex++) {
            OutputSlot<?> slotPattern = this.getOutput(outputIndex);
            final OutputSlot<?> testSlot = operator.getOutput(slotPattern.getIndex());
            if (!this.matchSlot(slotPattern, testSlot)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Test whether a given test {@link Slot} matches a pattern {@link Slot}.
     *
     * @param slotPattern that should be matched against
     * @param testSlot    will be matched
     * @return whether the {@code testSlot} matches the {@code slotPattern}
     */
    private boolean matchSlot(Slot<?> slotPattern, Slot<?> testSlot) {
        return slotPattern.getType().isNone() || slotPattern.getType().isSupertypeOf(testSlot.getType());
    }

    /**
     * Test whether the {@link #additionalTests} are satisfied.
     *
     * @param operator that should be tested
     * @return the tests are satisfied
     */
    private boolean matchAdditionalTests(T operator) {
        return this.additionalTests.stream().allMatch(test -> test.test((T) operator));
    }

    private void checkSanity(Operator operator) {
        if (this.getNumRegularInputs() != operator.getNumRegularInputs()) {
            throw new IllegalStateException(String.format("%s expected %d inputs, but matched %s with %d inputs.",
                    this, this.getNumRegularInputs(), operator, operator.getNumRegularInputs()));
        }
        if (this.getNumOutputs() != operator.getNumOutputs()) {
            throw new IllegalStateException("Matched an operator with different numbers of outputs.");
        }
    }

    /**
     * Add an additional {@link Predicate} that must be satisfied in order to establish matches with {@link Operator}s.
     *
     * @param additionalTest the {@link Predicate}
     * @return this instance
     */
    public OperatorPattern<T> withAdditionalTest(Predicate<T> additionalTest) {
        this.additionalTests.add(additionalTest);
        return this;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public <Payload, Return> Return accept(TopDownPlanVisitor<Payload, Return> visitor, OutputSlot<?> outputSlot, Payload payload) {
        throw new RuntimeException("Pattern does not accept visitors.");
    }

    @Override
    public String toString() {
        return String.format("%s[%d->%d, %s, id=%x]",
                this.getClass().getSimpleName(),
                this.getNumInputs(),
                this.getNumOutputs(),
                this.operatorClass.getSimpleName(),
                this.hashCode());
    }
}
