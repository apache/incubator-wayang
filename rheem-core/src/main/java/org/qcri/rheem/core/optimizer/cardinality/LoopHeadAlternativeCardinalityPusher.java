package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.util.Tuple;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * {@link CardinalityPusher} implementation for {@link LoopHeadAlternative}s.
 */
public class LoopHeadAlternativeCardinalityPusher extends AbstractAlternativeCardinalityPusher {

    Collection<Tuple<OperatorAlternative.Alternative, CardinalityPusher>> alternativePushers;

    public LoopHeadAlternativeCardinalityPusher(
            final LoopHeadAlternative loopHeadAlternative,
            Collection<InputSlot<?>> relevantInputSlots,
            Collection<OutputSlot<?>> relevantOutputSlots,
            BiFunction<OperatorAlternative.Alternative, Configuration, CardinalityPusher> pusherRetriever,
            final Configuration configuration
    ) {
        super(Slot.toIndices(relevantInputSlots), Slot.toIndices(relevantOutputSlots));
        this.alternativePushers = loopHeadAlternative.getAlternatives().stream()
                .map(alternative -> {
                    final CardinalityPusher alternativePusher = pusherRetriever.apply(alternative, configuration);
                    return new Tuple<>(alternative, alternativePusher);
                })
                .collect(Collectors.toList());
    }


    @Override
    public void pushThroughAlternatives(OptimizationContext.OperatorContext opCtx, Configuration configuration) {
        final OptimizationContext optCtx = opCtx.getOptimizationContext();
        for (Tuple<OperatorAlternative.Alternative, CardinalityPusher> alternativePusher : this.alternativePushers) {
            LoopHeadOperator loopHeadOperator = (LoopHeadOperator) alternativePusher.field0.getContainedOperator();
            final OptimizationContext.OperatorContext lhoCtx = optCtx.getOperatorContext(loopHeadOperator);
            alternativePusher.field1.push(lhoCtx, configuration);
        }
    }

}
