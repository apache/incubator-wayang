package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.util.Tuple;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * {@link CardinalityPusher} implementation for {@link OperatorAlternative}s.
 */
public class AggregatingCardinalityPusher extends CardinalityPusher {

    /**
     * Maintains a {@link CardinalityPusher} for each {@link OperatorAlternative.Alternative}.
     */
    private final List<Tuple<OperatorAlternative.Alternative, CardinalityPusher>> pushPaths;

    public AggregatingCardinalityPusher(final OperatorAlternative operatorAlternative,
                                        final Configuration configuration) {
        super(operatorAlternative);
        this.pushPaths = this.initializePushPaths(operatorAlternative, Operator::getCardinalityPusher, configuration);
    }

    public AggregatingCardinalityPusher(final OperatorAlternative operatorAlternative,
                                        Collection<InputSlot<?>> relevantInputSlots,
                                        Collection<OutputSlot<?>> relevantOutputSlots,
                                        BiFunction<Operator, Configuration, CardinalityPusher> getPusherFunction,
                                        final Configuration configuration) {
        super(Slot.toIndices(relevantInputSlots), Slot.toIndices(relevantOutputSlots));
        this.pushPaths = this.initializePushPaths(operatorAlternative, getPusherFunction, configuration);
    }

    private List<Tuple<OperatorAlternative.Alternative, CardinalityPusher>> initializePushPaths(
            OperatorAlternative operatorAlternative,
            BiFunction<Operator, Configuration, CardinalityPusher> getPusherFunction,
            Configuration configuration) {
        return operatorAlternative.getAlternatives().stream()
                .map(alternative -> {
                    final CardinalityPusher pusher = getPusherFunction.apply(alternative.getOperator(), configuration);
                    return new Tuple<>(alternative, pusher);
                })
                .collect(Collectors.toList());
    }

    @Override
    protected boolean canUpdate(OptimizationContext.OperatorContext operatorContext) {
        // We always try to update because there might be internal updates.
        return true;
    }

    @Override
    protected void doPush(OptimizationContext.OperatorContext opCtx, Configuration configuration) {
        final OptimizationContext optimizationCtx = opCtx.getOptimizationContext();

        // Trigger the push for each of the pushPaths.
        this.pushPaths.forEach(pushPath -> this.pushThroughPath(pushPath, configuration, optimizationCtx));

        // Somehow merge the CardinalityEstimates from the pushPaths to the final ones for the opCtx.
        this.pickCardinalities(opCtx);
    }

    /**
     * Trigger the {@link CardinalityPusher} for the given {@code pushPath}.
     */
    private void pushThroughPath(Tuple<OperatorAlternative.Alternative, CardinalityPusher> pushPath,
                                 Configuration configuration,
                                 OptimizationContext optimizationCtx) {
        // Identify the OperatorContext for the pushPath.
        final OptimizationContext.OperatorContext operatorCtx = this.getOperatorContext(pushPath, optimizationCtx);

        // Perform the push.
        pushPath.field1.push(operatorCtx, configuration);
        operatorCtx.pushCardinalitiesForward();
    }

    /**
     * Retrieves the {@link OptimizationContext.OperatorContext} for the {@code pushPath} (should reside within the
     * {@code optimizationCtx}).
     */
    private OptimizationContext.OperatorContext getOperatorContext(
            Tuple<OperatorAlternative.Alternative, CardinalityPusher> pushPath,
            OptimizationContext optimizationCtx) {
        final Operator alternativeOperator = pushPath.getField0().getOperator();
        return optimizationCtx.getOperatorContext(alternativeOperator);
    }

    /**
     * Pick {@link CardinalityEstimate}s for each of the {@link OutputSlot}s within the {@code opCtx}.
     */
    private void pickCardinalities(OptimizationContext.OperatorContext opCtx) {
        // Merge the cardinalities of the pushPaths using mergedEstimates(...)
        final OperatorAlternative operatorAlternative = (OperatorAlternative) opCtx.getOperator();
        final CardinalityEstimate[] mergedCardinalities = this.pushPaths.stream()
                .map(pushPath -> this.getOperatorContext(pushPath, opCtx.getOptimizationContext()))
                .map(OptimizationContext.OperatorContext::getOutputCardinalities)
                .reduce(this::mergeEstimates)
                .orElseThrow(() -> new AssertionError("Push paths did not deliver cardinalities for " + operatorAlternative));

        // Copy the mergedCardinalities to the opCtx.
        assert opCtx.getOutputCardinalities().length == mergedCardinalities.length;
        for (int outputIndex = 0; outputIndex < mergedCardinalities.length; outputIndex++) {
            opCtx.setOutputCardinality(outputIndex, mergedCardinalities[outputIndex]);
        }
    }

    /**
     * Merge two {@link CardinalityEstimate} vectors (point-wise).
     * <p>TODO: Check if this is a good way. There are other palpable approaches (e.g., weighted average).</p>
     */
    private CardinalityEstimate[] mergeEstimates(CardinalityEstimate[] estimates1, CardinalityEstimate[] estimates2) {
        // TODO: Check if this is a good way. There are other palpable approaches (e.g., weighted average).
        assert estimates1.length == estimates2.length;
        CardinalityEstimate[] mergedEstimates = Arrays.copyOf(estimates1, estimates1.length);

        // TODO: Push cardinalities down.
        for (int i = 0; i < estimates1.length; i++) {
            CardinalityEstimate estimate1 = estimates1[i];
            CardinalityEstimate estimate2 = estimates2[i];
            if (estimate1 == null) {
                assert estimate2 == null;
                continue;
            }
            if (estimate1.isOverride()) continue;
            if (estimate2.getCorrectnessProbability() > estimate1.getCorrectnessProbability() || estimate2.isOverride()) {
                mergedEstimates[i] = estimate2;
            }
        }
        return mergedEstimates;
    }

}
