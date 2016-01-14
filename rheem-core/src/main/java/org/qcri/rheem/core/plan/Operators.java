package org.qcri.rheem.core.plan;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Utility class for {@link Operator}s.
 */
public class Operators {

    /**
     * Find the innermost common parent of two operators.
     */
    public static Operator getCommonParent(Operator o1, Operator o2) {
        Operator commonParent = null;

        final Iterator<Operator> i1 = collectParents(o1, false).iterator();
        final Iterator<Operator> i2 = collectParents(o2, false).iterator();

        while (i1.hasNext() && i2.hasNext()) {
            final Operator parent1 = i1.next(), parent2 = i2.next();
            if (parent1 != parent2) break;
            commonParent = parent1;
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

}
