package org.qcri.rheem.core.plan.rheemplan;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Utility class for {@link Operator}s.
 */
public class Operators {

    /**
     * Find the innermost common {@link OperatorContainer} of two operators.
     *
     * @return the common {@link OperatorContainer} or {@code null} if none
     */
    public static OperatorContainer getCommonContainer(Operator o1, Operator o2) {
        OperatorContainer commonContainer = null;

        final Iterator<OperatorContainer> i1 = collectContainers(o1).iterator();
        final Iterator<OperatorContainer> i2 = collectContainers(o2).iterator();

        while (i1.hasNext() && i2.hasNext()) {
            final OperatorContainer container1 = i1.next(), container2 = i2.next();
            if (container1 != container2) break;
            commonContainer = container1;
        }

        return commonContainer;
    }

    /**
     * Creates the hierachy of an operators wrt. {@link Operator#getParent()}.
     *
     * @return the hierarchy with the first element being the top-level/outermost operator
     */
    public static List<Operator> collectParents(Operator operator, boolean includeSelf) {
        List<Operator> result = new LinkedList<>();
        if (!includeSelf) operator = operator.getParent();
        while (operator != null) {
            result.add(operator);
            operator = operator.getParent();
        }
        Collections.reverse(result);
        return result;
    }

    /**
     * Creates the hierachy of an operators wrt. {@link Operator#getContainer()}.
     *
     * @return the hierarchy with the first element being the top-level/outermost container
     */
    public static List<OperatorContainer> collectContainers(Operator operator) {
        List<OperatorContainer> result = new LinkedList<>();
        while (operator != null) {
            final OperatorContainer container = operator.getContainer();
            if (container != null) {
                result.add(container);
            }
            operator = operator.getParent();
        }
        Collections.reverse(result);
        return result;
    }

    /**
     * Compares the inputs of two operators and passes quietly if they are identical.
     *
     * @throws IllegalArgumentException if the operators differ in their inputs
     */
    public static void assertEqualInputs(Operator o1, Operator o2) throws IllegalArgumentException {
        if (o1.getNumInputs() != o2.getNumInputs()) {
            throw new IllegalArgumentException("Operators have different numbers of inputs.");
        }

        for (int i = 0; i < o1.getNumInputs(); i++) {
            final InputSlot<?> input1 = o1.getInput(i);
            final InputSlot<?> input2 = o2.getInput(i);
            if ((input1 == null && input2 != null) ||
                    (input1 != null && input2 == null) ||
                    (input1 != null && input2 != null && !input1.getType().equals(input2.getType()))) {
                throw new IllegalArgumentException("Operators differ in input " + i + ".");
            }
        }
    }

    /**
     * Compares the outputs of two operators and passes quietly if they are identical.
     *
     * @throws IllegalArgumentException if the operators differ in their outputs
     */
    public static void assertEqualOutputs(Operator o1, Operator o2) throws IllegalArgumentException {
        if (o1.getNumOutputs() != o2.getNumOutputs()) {
            throw new IllegalArgumentException("Operators have different numbers of outputs.");
        }

        for (int i = 0; i < o1.getNumOutputs(); i++) {
            final OutputSlot<?> output1 = o1.getOutput(i);
            final OutputSlot<?> output2 = o2.getOutput(i);
            if ((output1 == null && output2 != null) ||
                    (output1 != null && output2 == null) ||
                    (output1 != null && output2 != null && !output1.getType().equals(output2.getType()))) {
                throw new IllegalArgumentException("Operators differ in output " + i + ".");
            }
        }
    }

    public static final Operator slotlessOperator(OperatorContainer container) {
        return new OperatorBase(0, 0, container) {
            @Override
            public <Payload, Return> Return accept(TopDownPlanVisitor<Payload, Return> visitor, OutputSlot<?> outputSlot, Payload payload) {
                throw new RuntimeException("Not implemented.");
            }
        };
    }

    public static final Operator slotlessOperator() {
        return slotlessOperator(null);
    }
}
