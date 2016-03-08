package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.platform.Junction;

import java.util.LinkedList;
import java.util.List;

/**
 * Describes the enumeration of a {@link LoopSubplan}.
 */
public class LoopEnumeration {

    private final LoopSubplan enumeratedLoop;

    private final List<IterationEnumeration> iterationEnumerations = new LinkedList<>();

    public LoopEnumeration(LoopSubplan enumeratedLoop) {
        this.enumeratedLoop = enumeratedLoop;
    }

    public class IterationEnumeration {

        private int numIterations;

        private PlanEnumeration bodyEnumeration;

        private Junction interBodyJunction;

        private Junction interIterationJunction;

        private Junction loopOutputJunction;

    }

}
