package org.apache.incubator.wayang.core.optimizer.cardinality;

import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.optimizer.OptimizationContext;
import org.apache.incubator.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.incubator.wayang.core.util.Tuple;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link CardinalityPusher} implementation for {@link OperatorAlternative}s.
 */
public class OperatorAlternativeCardinalityPusher extends AbstractAlternativeCardinalityPusher {

    /**
     * Maintains a {@link CardinalityEstimationTraversal} for each {@link OperatorAlternative.Alternative}.
     */
    private final List<Tuple<OperatorAlternative.Alternative, CardinalityEstimationTraversal>> alternativeTraversals;

    public OperatorAlternativeCardinalityPusher(final OperatorAlternative operatorAlternative,
                                                final Configuration configuration
    ) {
        super(operatorAlternative);
        this.alternativeTraversals = operatorAlternative.getAlternatives().stream()
                .map(alternative -> {
                    final CardinalityEstimationTraversal traversal =
                            CardinalityEstimationTraversal.createPushTraversal(alternative, configuration);
                    return new Tuple<>(alternative, traversal);
                })
                .collect(Collectors.toList());
    }

    @Override
    public void pushThroughAlternatives(OptimizationContext.OperatorContext opCtx, Configuration configuration) {
        final OptimizationContext optimizationContext = opCtx.getOptimizationContext();
        for (Tuple<OperatorAlternative.Alternative, CardinalityEstimationTraversal> alternativeTraversal :
                this.alternativeTraversals) {
            this.pushThroughPath(alternativeTraversal, configuration, optimizationContext);
        }
    }

    /**
     * Trigger the {@link CardinalityEstimationTraversal} for the given {@code traversal}.
     */
    private void pushThroughPath(Tuple<OperatorAlternative.Alternative, CardinalityEstimationTraversal> traversal,
                                 Configuration configuration,
                                 OptimizationContext optimizationCtx) {
        // Perform the push.
        traversal.field1.traverse(optimizationCtx, configuration);
    }

}
