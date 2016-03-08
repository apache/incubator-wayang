package org.qcri.rheem.core.platform;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;

/**
 * {@link Breakpoint} implementation that is based on the {@link CardinalityEstimate}s of {@link Channel}s.
 * <p>Specifically, this implementation requires that <i>all</i> {@link CardinalityEstimate}s of the inbound
 * {@link Channel}s of an {@link ExecutionStage} are to a certain extent accurate within a given probability.</p>
 */
public class CardinalityBreakpoint implements Breakpoint {

    private static final double SPREAD_SMOOTHING = 10000d;

    private final double minConfidence;

    private final double maxSpread;

    /**
     * Creates a new instance.
     *
     * @param minConfidence the minimum confidence of the {@link CardinalityEstimate}s
     * @param maxSpread     the minimum accuracy of the {@link CardinalityEstimate}s
     */
    public CardinalityBreakpoint(double minConfidence, double maxSpread) {
        Validate.inclusiveBetween(0, 1, minConfidence);
        Validate.isTrue(maxSpread >= 1);
        this.minConfidence = minConfidence;
        this.maxSpread = maxSpread;
    }

    @Override
    public boolean permitsExecutionOf(ExecutionStage stage) {
//        throw new RuntimeException("todo");
//        return stage.getInboundChannels().stream().map(Channel::getCardinalityEstimate).allMatch(this::approves);
        return true;
    }

    private boolean approves(CardinalityEstimate cardinalityEstimate) {
        return cardinalityEstimate.getCorrectnessProbability() >= this.minConfidence
                && this.calculateSpread(cardinalityEstimate) <= this.maxSpread;
    }

    public static double calculateSpread(CardinalityEstimate cardinalityEstimate) {
        return ((double) cardinalityEstimate.getUpperEstimate() + SPREAD_SMOOTHING)
                / ((double) cardinalityEstimate.getLowerEstimate() + SPREAD_SMOOTHING);
    }

}
