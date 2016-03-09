package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.plan.rheemplan.Operator;
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

    public IterationEnumeration addIterationEnumeration(int numIterations, PlanEnumeration bodyEnumeration) {
        IterationEnumeration iterationEnumeration = new IterationEnumeration(numIterations, bodyEnumeration);
        this.iterationEnumerations.add(iterationEnumeration);
        return iterationEnumeration;
    }

    /**
     * Enumeration for a number of contiguous loop iterations.
     */
    public static class IterationEnumeration {

        /**
         * The number of iterations performed with this enumeration.
         */
        private final int numIterations;

        /**
         * The {@link PlanEnumeration} of the loop body (from the {@link LoopHeadOperator} to the final loop
         * {@link Operator}.
         */
        private final PlanEnumeration bodyEnumeration;

        /**
         * Connects two iterations with this instance.
         */
        private Junction interBodyJunction;

        /**
         * Connects an iteration of this instance with the iteration of a different instance.
         */
        private Junction forwardJunction;

        /**
         * Connects the iteration with the outside {@link PlanEnumeration}. Notice that this is in general
         * required for all iterations if there are "side inputs".
         */
        private Junction enterJunction;

        /**
         * Connects the final iteration with the outside {@link PlanEnumeration}. Notice that this is in general
         * required for all iterations as {@link #numIterations} might be overestimated.
         */
        private Junction exitJunction;

        public IterationEnumeration(int numIterations, PlanEnumeration bodyEnumeration) {
            this.numIterations = numIterations;
            this.bodyEnumeration = bodyEnumeration;
        }

        public int getNumIterations() {
            return this.numIterations;
        }

        public PlanEnumeration getBodyEnumeration() {
            return this.bodyEnumeration;
        }

        public Junction getInterBodyJunction() {
            return this.interBodyJunction;
        }

        public void setInterBodyJunction(Junction interBodyJunction) {
            this.interBodyJunction = interBodyJunction;
        }

        public Junction getForwardJunction() {
            return this.forwardJunction;
        }

        public void setForwardJunction(Junction forwardJunction) {
            this.forwardJunction = forwardJunction;
        }

        public Junction getEnterJunction() {
            return this.enterJunction;
        }

        public void setEnterJunction(Junction enterJunction) {
            this.enterJunction = enterJunction;
        }

        public Junction getExitJunction() {
            return this.exitJunction;
        }

        public void setExitJunction(Junction exitJunction) {
            this.exitJunction = exitJunction;
        }
    }

}
