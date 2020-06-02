package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.Junction;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Describes the enumeration of a {@link LoopSubplan}.
 */
public class LoopImplementation {

    private final LoopSubplan enumeratedLoop;

    private final List<IterationImplementation> iterationImplementations = new LinkedList<>();

    public LoopImplementation(LoopSubplan enumeratedLoop) {
        this.enumeratedLoop = enumeratedLoop;
    }

    /**
     * Copy constructor. Creates instance copies of the {@link IterationImplementation}.
     *
     * @param original instance to be copied
     */
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

    /**
     * Retrieve the {@link TimeEstimate} for this instance. Global overhead is not included.
     *
     * @return the {@link TimeEstimate}
     */
    public TimeEstimate getTimeEstimate() {
        // What about the Junctions? Are they already included?
        // Yes, in-loop Junctions are contained in the body implementations and the surrounding Junctions are
        // contained in the top-level PlanImplementation.
        TimeEstimate timeEstimate = TimeEstimate.ZERO;
        for (int i = 0; i < this.iterationImplementations.size(); i++) {
            timeEstimate = timeEstimate.plus(this.iterationImplementations.get(i).getTimeEstimate());
        }
        return timeEstimate;
    }

    /**
     * Retrieve the cost estimate for this instance. Fix costs are not excluded.
     *
     * @return the cost estimate
     */
    public ProbabilisticDoubleInterval getCostEstimate() {
        // What about the Junctions? Are they already included?
        // Yes, in-loop Junctions are contained in the body implementations and the surrounding Junctions are
        // contained in the top-level PlanImplementation.
        ProbabilisticDoubleInterval costEstimate = ProbabilisticDoubleInterval.zero;
        for (int i = 0; i < this.iterationImplementations.size(); i++) {
            costEstimate = costEstimate.plus(this.iterationImplementations.get(i).getCostEstimate());
        }
        return costEstimate;
    }

    /**
     * Retrieve the squashed cost estimate for this instance. Fix costs are not excluded.
     *
     * @return the squashed cost estimate
     */
    public double getSquashedCostEstimate() {
        // What about the Junctions? Are they already included?
        // Yes, in-loop Junctions are contained in the body implementations and the surrounding Junctions are
        // contained in the top-level PlanImplementation.
        double costEstimate = 0d;
        for (int i = 0; i < this.iterationImplementations.size(); i++) {
            costEstimate += this.iterationImplementations.get(i).getSquashedCostEstimate();
        }
        return costEstimate;
    }

    public List<IterationImplementation> getIterationImplementations() {
        return this.iterationImplementations;
    }

    /**
     * Originally, only a single {@link IterationImplementation} is supported by Rheem. This method explicitly
     * captures this assumption.
     *
     * @return the single {@link IterationImplementation}
     */
    public IterationImplementation getSingleIterationImplementation() {
        if (this.iterationImplementations.size() != 1) {
            throw new AssertionError("Expected only a single iteration implementation. Has this changed?");
        }
        return this.iterationImplementations.get(0);
    }

    /**
     * Stream all the {@link ExecutionOperator}s in this instance.
     *
     * @return a {@link Stream} containing every iteration-body {@link ExecutionOperator} at least once
     */
    Stream<ExecutionOperator> streamOperators() {
        // It is sufficient to take the first IterationImplementation to see all ExecutionOperators.
        return this.getSingleIterationImplementation().streamOperators();
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

            // Seems like we are not using these fields currently.
            this.interBodyJunction = originalIteration.interBodyJunction;
            this.forwardJunction = originalIteration.forwardJunction;
            this.enterJunction = originalIteration.enterJunction;
            this.exitJunction = originalIteration.exitJunction;

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

        /**
         * Retrieve the {@link TimeEstimate} for this instance. Global overhead is not included.
         *
         * @return the {@link TimeEstimate}
         */
        public TimeEstimate getTimeEstimate() {
            return this.bodyImplementation.getTimeEstimate(false);
        }

        /**
         * Retrieve the cost estimate for this instance. Global overhead is not included.
         *
         * @return the cost estimate
         */
        public ProbabilisticDoubleInterval getCostEstimate() {
            return this.bodyImplementation.getCostEstimate(false);
        }

        /**
         * Retrieve the cost estimate for this instance. Global overhead is not included.
         *
         * @return the cost estimate
         */
        public double getSquashedCostEstimate() {
            return this.bodyImplementation.getSquashedCostEstimate(false);
        }

        /**
         * Stream all the {@link ExecutionOperator}s in this instance.
         *
         * @return a {@link Stream} containing every iteration-body {@link ExecutionOperator} at least once
         */
        Stream<ExecutionOperator> streamOperators() {
            return this.bodyImplementation.streamOperators();
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
