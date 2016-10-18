package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OperatorAlternative;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;

/**
 * Abstract {@link CardinalityPusher} implementation for {@link OperatorAlternative}s and subclasses.
 */
public abstract class AbstractAlternativeCardinalityPusher extends CardinalityPusher {

    /**
     * @see CardinalityPusher#CardinalityPusher(Operator)
     */
    public AbstractAlternativeCardinalityPusher(Operator operator) {
        super(operator);
    }

    /**
     * @see CardinalityPusher#CardinalityPusher(int[], int[])
     */
    public AbstractAlternativeCardinalityPusher(int[] relevantInputIndices, int[] relevantOutputIndices) {
        super(relevantInputIndices, relevantOutputIndices);
    }

    @Override
    protected boolean canUpdate(OptimizationContext.OperatorContext operatorContext) {
        // We always try to update because there might be internal updates.
        return true;
    }

    @Override
    protected void doPush(OptimizationContext.OperatorContext opCtx, Configuration configuration) {
        // Trigger the push for each of the alternativeTraversals.
        this.pushThroughAlternatives(opCtx, configuration);

        // Somehow merge the CardinalityEstimates from the alternativeTraversals to the final ones for the opCtx.
        this.pickCardinalities(opCtx);
    }

    /**
     * Do the pushing through all of the alternatives.
     *
     * @param opCtx         as in {@link #doPush(OptimizationContext.OperatorContext, Configuration)}
     * @param configuration as in {@link #doPush(OptimizationContext.OperatorContext, Configuration)}
     */
    public abstract void pushThroughAlternatives(OptimizationContext.OperatorContext opCtx, Configuration configuration);


    /**
     * Pick {@link CardinalityEstimate}s for each of the {@link OutputSlot}s within the {@code opCtx}.
     */
    protected void pickCardinalities(OptimizationContext.OperatorContext opCtx) {
        final OperatorAlternative operatorAlternative = (OperatorAlternative) opCtx.getOperator();

        // For each relevant OutputSlot of the OperatorAlternative...
        for (int outputIndex : this.relevantOutputIndices) {
            final OutputSlot<?> output = operatorAlternative.getOutput(outputIndex);
            CardinalityEstimate bestEstimate = opCtx.getOutputCardinality(output.getIndex());

            // ...and dor each corresponding OutputSlot in each Alternative...
            for (OperatorAlternative.Alternative alternative : operatorAlternative.getAlternatives()) {

                // ...pick the best CardinalityEstimate.
                final OutputSlot<?> innerOutput = alternative.getSlotMapping().resolveUpstream(output);
                if (innerOutput == null) continue;
                final OptimizationContext.OperatorContext innerOpCtx = opCtx.getOptimizationContext()
                        .getOperatorContext(innerOutput.getOwner());
                final CardinalityEstimate newEstimate = innerOpCtx.getOutputCardinality(innerOutput.getIndex());
                if (newEstimate == null) {
                    logger.warn("No cardinality estimate for {}.", innerOutput);
                    continue;
                }
                bestEstimate = this.choose(bestEstimate, newEstimate);
            }

            // Finalize the decision.
            opCtx.setOutputCardinality(output.getIndex(), bestEstimate);
        }
    }

    /**
     * Merge two {@link CardinalityEstimate} vectors (point-wise).
     */
    protected CardinalityEstimate choose(CardinalityEstimate estimate1, CardinalityEstimate estimate2) {
        // Make sure there are no nulls.
        if (estimate1 == null) {
            return estimate2;
        } else if (estimate2 == null) {
            return estimate1;
        }

        // Check for overrides.
        if (estimate1.isOverride()) return estimate1;
        if (estimate2.isOverride()) return estimate2;

        // Actually compare the two estimates.
        // TODO: Check if this is a good way. There are other palpable approaches (e.g., weighted average).
        if (estimate2.getCorrectnessProbability() > estimate1.getCorrectnessProbability()) {
            return estimate2;
        }
        return estimate1;
    }

}
