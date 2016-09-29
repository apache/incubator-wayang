package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;

/**
 * This (linear) converter turns {@link TimeEstimate}s into cost estimates.
 */
public class TimeToCostConverter {

    /**
     * The fix costs to be paid in some context.
     */
    private final double fixCosts;

    /**
     * The costs per millisecond.
     */
    private final double costsPerMilli;

    /**
     * Creates a new instance
     *
     * @param fixCosts      the fix costs to be paid in some context.
     * @param costsPerMilli the costs per millisecond
     */
    public TimeToCostConverter(double fixCosts, double costsPerMilli) {
        this.fixCosts = fixCosts;
        this.costsPerMilli = costsPerMilli;
    }

    /**
     * Convert the given {@link TimeEstimate} into a cost estimate.
     *
     * @param timeEstimate the {@link TimeEstimate}
     * @return the cost estimate
     */
    public ProbabilisticDoubleInterval convert(TimeEstimate timeEstimate) {
        double lowerBound = this.fixCosts + this.costsPerMilli * timeEstimate.getLowerEstimate();
        double upperBound = this.fixCosts + this.costsPerMilli * timeEstimate.getUpperEstimate();
        return new ProbabilisticDoubleInterval(lowerBound, upperBound, timeEstimate.getCorrectnessProbability());
    }


    /**
     * Convert the given {@link TimeEstimate} into a cost estimate without considering the fix costs.
     *
     * @param timeEstimate the {@link TimeEstimate}
     * @return the cost estimate
     */
    public ProbabilisticDoubleInterval convertWithoutFixCosts(TimeEstimate timeEstimate) {
        double lowerBound = this.costsPerMilli * timeEstimate.getLowerEstimate();
        double upperBound = this.costsPerMilli * timeEstimate.getUpperEstimate();
        return new ProbabilisticDoubleInterval(lowerBound, upperBound, timeEstimate.getCorrectnessProbability());
    }

    /**
     * Get the fix costs, i.e., the overhead, that incur according to this instance.
     *
     * @return the fix costs
     */
    public double getFixCosts() {
        return this.fixCosts;
    }

    /**
     * Get the costs that incur per millisecond according to this instance.
     *
     * @return the costs per milliseond
     */
    public double getCostsPerMillisecond() {
        return this.costsPerMilli;
    }
}
