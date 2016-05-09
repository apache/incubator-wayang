package org.qcri.rheem.core.platform;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;

/**
 * {@link Breakpoint} implementation that is based on the {@link CardinalityEstimate}s of {@link Channel}s.
 * <p>Specifically, this implementation requires that <i>all</i> {@link CardinalityEstimate}s of the inbound
 * {@link Channel}s of an {@link ExecutionStage} are to a certain extent accurate within a given probability.</p>
 */
public class CardinalityBreakpoint implements Breakpoint {

    private final double spreadSmoothing;

    private final double minConfidence;

    private final double maxSpread;

    /**
     * Creates a new instance.
     *
     * @param configuration provides configuration properties
     */
    public CardinalityBreakpoint(Configuration configuration) {
        this(
                configuration.getDoubleProperty("rheem.core.optimizer.cardinality.minconfidence"),
                configuration.getDoubleProperty("rheem.core.optimizer.cardinality.maxspread"),
                configuration.getDoubleProperty("rheem.core.optimizer.cardinality.spreadsmoothing")
        );
    }

    /**
     * Creates a new instance.
     *
     * @param minConfidence the minimum confidence of the {@link CardinalityEstimate}s
     * @param maxSpread     the minimum accuracy of the {@link CardinalityEstimate}s
     */
    public CardinalityBreakpoint(double minConfidence, double maxSpread, double spreadSmoothing) {
        Validate.inclusiveBetween(0, 1, minConfidence);
        Validate.isTrue(maxSpread >= 1);
        this.minConfidence = minConfidence;
        this.maxSpread = maxSpread;
        this.spreadSmoothing = spreadSmoothing;
    }

    @Override
    public boolean permitsExecutionOf(ExecutionStage stage) {
        // todo
//        return stage.getInboundChannels().stream().map(Channel::getCardinalityEstimate).allMatch(this::approves);
        return true;
    }

    private boolean approves(CardinalityEstimate cardinalityEstimate) {
        return cardinalityEstimate.getCorrectnessProbability() >= this.minConfidence
                && calculateSpread(cardinalityEstimate) <= this.maxSpread;
    }

    public double calculateSpread(CardinalityEstimate cardinalityEstimate) {
        return ((double) cardinalityEstimate.getUpperEstimate() + this.spreadSmoothing)
                / ((double) cardinalityEstimate.getLowerEstimate() + this.spreadSmoothing);
    }

}
