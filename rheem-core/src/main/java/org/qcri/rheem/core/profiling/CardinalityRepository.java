package org.qcri.rheem.core.profiling;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.platform.CrossPlatformExecutor;
import org.qcri.rheem.core.platform.ExecutionProfile;
import org.qcri.rheem.core.util.Tuple;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Stores cardinalities that have been collected by the {@link CrossPlatformExecutor}.
 */
public class CardinalityRepository {

    private final String repositoryPath;

    public CardinalityRepository(String repositoryPath) {
        this.repositoryPath = repositoryPath;
    }

    /**
     * Store the input and output cardinalities for all those {@link Operator}s that have a measured output
     * cardinality.
     *
     * @param executionProfile contains the cardinalites of the instrumented {@link Slot}s; those cardinalities should
     *                         be already injected in the {@code rheemPlan}
     * @param rheemPlan        that has been executed; it is assumed, that any measured cardinalities are already
 *                         injected in this {@link RheemPlan} to guarantee that we capture the most possible
 *                         accurate data
     */
    public void storeAll(ExecutionProfile executionProfile, RheemPlan rheemPlan) {
        final Map<Channel, Long> cardinalities = executionProfile.getCardinalities();
        for (Map.Entry<Channel, Long> entry : cardinalities.entrySet()) {
            final Channel channel = entry.getKey();
            final long cardinality = entry.getValue();
            for (Slot<?> slot : channel.getCorrespondingSlots()) {
                if (slot instanceof OutputSlot<?>) {
                    this.store((OutputSlot<?>) slot, cardinality, rheemPlan);
                }
            }
        }
    }

    public void store(OutputSlot<?> output, long cardinality, RheemPlan rheemPlan) {
        assert output.getCardinalityEstimate().isExactly(cardinality)
                : String.format("Expected a measured cardinality for %s; found %s.", output, output.getCardinalityEstimate());

        final Operator owner = output.getOwner();
        final InputSlot<?>[] allInputs = owner.getAllInputs();

        final List<Tuple<InputSlot<Object>, CardinalityEstimate>> inputs = Arrays.stream(allInputs)
                .map(input -> new Tuple<>(input.unchecked(), input.getCardinalityEstimate()))
                .collect(Collectors.toList());
        System.out.format("Storing cardinality evidence: %s -> %s -> %d\n", inputs, owner, cardinality);
    }
}
