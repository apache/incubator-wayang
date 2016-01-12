package org.qcri.rheem.core.plan;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * This operator encapsulates operators that are alternative to each other.
 */
public class OperatorAlternative extends OperatorBase {

    private List<Operator> alternatives = new LinkedList<>();

    /**
     * Wraps an {@link Operator}:
     * Creates a new instance that mocks the interface (slots) of the given operator. Moreover, the given operator is
     * set up as the first alternative.
     *
     * @param operator operator to wrap
     * @param parent   parent of this operator or {@code null}
     */
    public OperatorAlternative(Operator operator, Operator parent) {
        super(operator.getNumInputs(), operator.getNumOutputs(), parent);
        InputSlot.mock(operator, this);
        OutputSlot.mock(operator, this);
        this.alternatives.add(operator);
    }

    public List<Operator> getAlternatives() {
        return Collections.unmodifiableList(this.alternatives);
    }

    @Override
    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

}
