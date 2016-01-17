package org.qcri.rheem.core.plan;

import java.util.*;

/**
 * Utility class for {@link Operator}s.
 */
public class Operators {

    /**
     * Find the innermost common parent of two operators.
     */
    public static CompositeOperator getCommonParent(Operator o1, Operator o2) {
        CompositeOperator commonParent = null;

        final Iterator<Operator> i1 = collectParents(o1, false).iterator();
        final Iterator<Operator> i2 = collectParents(o2, false).iterator();

        while (i1.hasNext() && i2.hasNext()) {
            final Operator parent1 = i1.next(), parent2 = i2.next();
            if (parent1 != parent2) break;
            commonParent = (CompositeOperator) parent1;
        }

        return commonParent;
    }

    /**
     * Creates the hierachy of an operators wrt. {@link Operator#getParent()}.
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
     * Compares the inputs of two operators and passes quietly if they are identical.
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

}
