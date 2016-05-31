package org.qcri.rheem.core.optimizer;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.enumeration.PlanEnumerationPruningStrategy;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.*;

import java.util.*;

/**
 * Utility methods for the optimization process.
 */
public class OptimizationUtils {

//    private static final Logger logger = LoggerFactory.getLogger(OptimizationUtils.class);

    /**
     * Determine the producing {@link OutputSlot} of this {@link Channel} that lies within a {@link RheemPlan}.
     * We follow non-RheemPlan {@link ExecutionOperator}s because they should merely forward data.
     */
    public static OutputSlot<?> findRheemPlanOutputSlotFor(Channel openChannel) {
        OutputSlot<?> producerOutput = null;
        Channel tracedChannel = openChannel;
        do {
            final ExecutionTask producer = tracedChannel.getProducer();
            final ExecutionOperator producerOperator = producer.getOperator();
            if (checkIfRheemPlanOperator(producerOperator)) {
                producerOutput = producer.getOutputSlotFor(tracedChannel);
            } else {
                assert producer.getNumInputChannels() == 1;
                tracedChannel = producer.getInputChannel(0);
            }
        } while (producerOutput == null);
        return producerOutput;
    }

    /**
     * Heuristically determines if an {@link ExecutionOperator} was specified in a {@link RheemPlan} or if
     * it has been inserted by Rheem in a later stage.
     *
     * @param operator should be checked
     * @return whether the {@code operator} is deemed to be user-specified
     */
    public static boolean checkIfRheemPlanOperator(ExecutionOperator operator) {
        // A non-RheemPlan operator is presumed to be "free floating" and completely unconnected. Connections are only
        // maintained via ExecutionTasks and Channels.
        return !(operator.getParent() == null
                && Arrays.stream(operator.getAllInputs())
                .map(InputSlot::getOccupant)
                .allMatch(Objects::isNull)
                && Arrays.stream(operator.getAllOutputs())
                .flatMap(outputSlot -> outputSlot.getOccupiedSlots().stream())
                .allMatch(Objects::isNull)
        );
    }

    /**
     * Finds the single input {@link Channel} of the given {@code channel}'s producing {@link ExecutionTask}.
     *
     * @param channel whose predecessor is requested
     * @return the preceeding {@link Channel}
     */
    public static Channel getPredecessorChannel(Channel channel) {
        final ExecutionTask producer = channel.getProducer();
        assert producer != null && producer.getNumInputChannels() == 1;
        return producer.getInputChannel(0);
    }

    /**
     * Creates a new {@link PlanEnumerationPruningStrategy} and configures it.
     *
     * @param strategyClass the {@link Class} of the {@link PlanEnumerationPruningStrategy}; must have a default constructor
     * @param configuration provides any potential configuration values
     * @return the configured {@link PlanEnumerationPruningStrategy} instance
     */
    public static <T extends PlanEnumerationPruningStrategy> T createPruningStrategy(Class<T> strategyClass, Configuration configuration) {
        try {
            final T strategy = strategyClass.newInstance();
            strategy.configure(configuration);
            return strategy;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RheemException(String.format("Could not create pruning strategy for %s.", strategyClass.getCanonicalName()), e);
        }
    }

    /**
     * Collects all {@link Slot}s that are related to the given {@link Slot} either by a {@link SlotMapping} or
     * by {@link OutputSlot}/{@link InputSlot} occupation.
     *
     * @param slot whose related {@link Slot}s are requested
     * @return the related {@link Slot}s including the given {@link Slot}
     */
    public static Set<Slot<?>> collectConnectedSlots(Slot<?> slot) {
        assert slot != null;
        if (slot instanceof InputSlot) {
            return collectConnectedSlots((InputSlot) slot);
        } else {
            assert slot instanceof OutputSlot;
            return collectConnectedSlots((OutputSlot) slot);
        }
    }

    /**
     * Collects all {@link Slot}s that are related to the given {@link InputSlot} either by a {@link SlotMapping} or
     * by {@link OutputSlot}/{@link InputSlot} occupation.
     *
     * @param input whose related {@link Slot}s are requested
     * @return the related {@link Slot}s including the given {@link InputSlot}
     */
    public static Set<Slot<?>> collectConnectedSlots(InputSlot<?> input) {
        Set<Slot<?>> result = new HashSet<>();

        final InputSlot<?> outerInput = input.getOwner().getOutermostInputSlot(input);
        Set<InputSlot<Object>> allInputs = outerInput.getOwner().collectMappedInputSlots(outerInput.unchecked());
        result.addAll(allInputs);

        final OutputSlot<?> outerOutput = outerInput.getOccupant();
        Set<OutputSlot<Object>> allOutputs = outerOutput == null ?
                Collections.emptySet() :
                outerOutput.getOwner().collectMappedOutputSlots(outerOutput.unchecked());
        result.addAll(allOutputs);

        return result;
    }

    /**
     * Collects all {@link Slot}s that are related to the given {@link OutputSlot} either by a {@link SlotMapping} or
     * by {@link OutputSlot}/{@link InputSlot} occupation.
     *
     * @param output whose related {@link Slot}s are requested
     * @return the related {@link Slot}s including the given {@link OutputSlot}
     */
    public static Set<Slot<?>> collectConnectedSlots(OutputSlot<?> output) {
        Set<Slot<?>> result = new HashSet<>();

        final Collection<OutputSlot<Object>> outerOutputs = output.getOwner().getOutermostOutputSlots(output.unchecked());
        for (OutputSlot<Object> outerOutput : outerOutputs) {
            result.addAll(outerOutput.getOwner().collectMappedOutputSlots(outerOutput));
            for (InputSlot<Object> outerInput : outerOutput.getOccupiedSlots()) {
                result.addAll(outerInput.getOwner().collectMappedInputSlots(outerInput.unchecked()));
            }
        }

        return result;
    }
}
