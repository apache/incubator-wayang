package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.Junction;

import java.util.LinkedList;
import java.util.List;

/**
 * Describes the enumeration of a {@link LoopSubplan}.
 */
public class LoopImplementation {

    private final LoopSubplan enumeratedLoop;

    private final List<IterationImplementation> iterationImplementations = new LinkedList<>();

    public LoopImplementation(LoopSubplan enumeratedLoop) {
        this.enumeratedLoop = enumeratedLoop;
    }

    public LoopImplementation(LoopImplementation original) {
        this.enumeratedLoop = original.enumeratedLoop;
        for (IterationImplementation originalIteration : original.getIterationImplementations()) {
            this.iterationImplementations.add(new IterationImplementation(originalIteration));
        }
    }

    public IterationImplementation addIterationEnumeration(int numIterations, PlanImplementation bodyImplementation) {
        IterationImplementation iterationImplementation = new IterationImplementation(numIterations, bodyImplementation);
        this.iterationImplementations.add(iterationImplementation);
        return iterationImplementation;
    }

    public TimeEstimate getTimeEstimate() {
        TimeEstimate timeEstimate = TimeEstimate.ZERO;
        for (int i = 0; i < this.iterationImplementations.size(); i++) {
            timeEstimate = timeEstimate.plus(this.iterationImplementations.get(i).getTimeEstimate());
        }
        return timeEstimate;
    }

    public List<IterationImplementation> getIterationImplementations() {
        return this.iterationImplementations;
    }

    /**
     * Enumeration for a number of contiguous loop iterations.
     */
    public class IterationImplementation {

        /**
         * The number of iterations performed with this enumeration.
         */
        private final int numIterations;

        /**
         * The {@link PlanImplementation} of the loop body (from the {@link LoopHeadOperator} to the final loop
         * {@link Operator}s.
         */
        private final PlanImplementation bodyImplementation;

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

        public IterationImplementation(int numIterations, PlanImplementation bodyImplementation) {
            this.numIterations = numIterations;
            this.bodyImplementation = bodyImplementation;
        }

        public IterationImplementation(IterationImplementation originalIteration) {
            this.numIterations = originalIteration.getNumIterations();
            this.bodyImplementation = new PlanImplementation(originalIteration.getBodyImplementation());

            this.interBodyJunction = originalIteration.getInterBodyJunction();
            this.forwardJunction = originalIteration.getForwardJunction();
            this.enterJunction = originalIteration.getEnterJunction();
            this.exitJunction = originalIteration.getExitJunction();

        }

        public int getNumIterations() {
            return this.numIterations;
        }

        public PlanImplementation getBodyImplementation() {
            return this.bodyImplementation;
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

        public TimeEstimate getTimeEstimate() {
            // TODO: Is this enough? Probably not.
            return this.bodyImplementation.getTimeEstimate();
        }

        /**
         * @return the encasing {@link LoopImplementation}
         */
        public LoopImplementation getLoopImplementation() {
            return LoopImplementation.this;
        }

        public IterationImplementation getSuccessorIterationImplementation() {
            final List<IterationImplementation> allImpls = this.getLoopImplementation().getIterationImplementations();
            final int thisIndex = allImpls.indexOf(this);
            assert thisIndex != -1;
            final int successorIndex = thisIndex + 1;
            if (successorIndex < allImpls.size()) {
                return allImpls.get(successorIndex);
            } else {
                return null;
            }
        }

        /**
         * Retrieves the {@link Junction} that implements the given {@code output}.
         */
        public Junction getJunction(OutputSlot<?> output) {
            return this.getBodyImplementation().getJunction(output);
        }
    }

}
