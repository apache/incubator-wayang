package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStageLoop;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.util.AbstractReferenceCountable;
import org.qcri.rheem.core.util.Formats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Implements the {@link ExecutionResource} handling as defined by {@link Executor}.
 */
public abstract class ExecutorTemplate extends AbstractReferenceCountable implements Executor {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Provides IDs to distinguish instances humanly readable.
     */
    private static final AtomicInteger idGenerator = new AtomicInteger(0);

    /**
     * The {@link CrossPlatformExecutor} that instruments this instance or {@code null} if none
     */
    private final CrossPlatformExecutor crossPlatformExecutor;

    /**
     * Resources being held by this instance.
     */
    private final Set<ExecutionResource> registeredResources = new HashSet<>();

    /**
     * ID of this instance.
     */
    private final int id = idGenerator.getAndIncrement();

    /**
     * Creates a new instance.
     *
     * @param crossPlatformExecutor the {@link CrossPlatformExecutor} that instruments this instance or {@code null} if none
     */
    protected ExecutorTemplate(CrossPlatformExecutor crossPlatformExecutor) {
        this.crossPlatformExecutor = crossPlatformExecutor;
    }

    @Override
    protected void disposeUnreferenced() {
        this.dispose();
    }

    @Override
    public void register(ExecutionResource resource) {
        if (!this.registeredResources.add(resource)) {
            this.logger.warn("Registered {} twice.", resource);
        }
    }

    @Override
    public void unregister(ExecutionResource resource) {
        if (!this.registeredResources.remove(resource)) {
            this.logger.warn("Could not unregister {}, as it was not registered.", resource);
        }
    }

    /**
     * If the given {@link ChannelInstance} has a measured cardinality, then register this cardinality in the
     * {@link #crossPlatformExecutor} with the corresponding {@link Channel} and all its siblings.
     *
     * @param channelInstance the said {@link ChannelInstance}
     */
    protected void addCardinalityIfNotInLoop(ChannelInstance channelInstance) {
        // Check if a cardinality was measured in the first place.
        final OptionalLong optionalCardinality = channelInstance.getMeasuredCardinality();
        if (!optionalCardinality.isPresent()) {
            if (channelInstance.getChannel().isMarkedForInstrumentation()) {
                this.logger.warn(
                        "No cardinality available for {}, although it was requested.", channelInstance.getChannel()
                );
            }
            return;
        }
        final long cardinality = optionalCardinality.getAsLong();

        // Make sure that the channelInstance is not inside of a loop.
        final Channel channel = channelInstance.getChannel();
        channel.withSiblings().forEach(c -> {
            if (!checkIfIsInLoopChannel(c)) {
                this.crossPlatformExecutor.addCardinalityMeasurement(c, cardinality);
            }
        });
    }

    /**
     * Checks whether the given {@link Channel} is inside of a {@link ExecutionStageLoop}.
     *
     * @param channel the said {@link Channel}
     * @return whether the {@link Channel} is in a {@link ExecutionStageLoop}
     */
    private static boolean checkIfIsInLoopChannel(Channel channel) {
        final ExecutionStageLoop producerLoop = channel.getProducer().getStage().getLoop();
        return producerLoop != null && channel.getConsumers().stream().anyMatch(
                consumer -> consumer.getStage().getLoop() == producerLoop
        );
    }

    /**
     * Create a {@link PartialExecution} according to the given parameters.
     *
     * @param executedOperatorContexts {@link ExecutionOperator}s' {@link OptimizationContext.OperatorContext}s that
     *                                 have been executed
     * @param executionDuration        the measured execution duration in milliseconds
     * @return the {@link PartialExecution} or {@link null} if nothing has been executed
     */
    protected PartialExecution createPartialExecution(
            Collection<OptimizationContext.OperatorContext> executedOperatorContexts,
            long executionDuration) {

        if (executedOperatorContexts.isEmpty()) return null;

        final PartialExecution partialExecution = PartialExecution.createFromMeasurement(
                executionDuration, executedOperatorContexts, this.getConfiguration()
        );
        if (this.logger.isInfoEnabled()) {
            this.logger.info(
                    "Executed {} operator(s) in {} (estimated {}): {}",
                    executedOperatorContexts.size(),
                    Formats.formatDuration(partialExecution.getMeasuredExecutionTime()),
                    partialExecution.getOverallTimeEstimate(),
                    partialExecution.getOperatorContexts().stream()
                            .map(opCtx -> String.format(
                                    "%s(time=%s, cards=%s)",
                                    opCtx.getOperator(), opCtx.getTimeEstimate(), formatCardinalities(opCtx)
                            ))
                            .collect(Collectors.toList())
            );
        }

        return partialExecution;
    }

    private static String formatCardinalities(OptimizationContext.OperatorContext opCtx) {
        StringBuilder sb = new StringBuilder().append('[');
        String separator = "";
        final CardinalityEstimate[] inputCardinalities = opCtx.getInputCardinalities();
        for (int inputIndex = 0; inputIndex < inputCardinalities.length; inputIndex++) {
            if (inputCardinalities[inputIndex] != null) {
                String slotName = opCtx.getOperator().getNumInputs() > inputIndex ?
                        opCtx.getOperator().getInput(inputIndex).getName() :
                        "(none)";
                sb.append(separator).append(slotName).append(": ").append(inputCardinalities[inputIndex]);
                separator = ", ";
            }
        }
        return sb.append(']').toString();
    }

    @Override
    public void dispose() {
        if (this.getNumReferences() != 0) {
            this.logger.warn("Disposing {} although it is still being referenced.", this);
        }

        for (ExecutionResource resource : new ArrayList<>(this.registeredResources)) {
            resource.dispose();
        }

        if (this.getNumReferences() > 0) {
            this.logger.warn("There are still {} referenced on {}, which is about to be disposed.", this.getNumReferences(), this);
        }
    }

    @Override
    public CrossPlatformExecutor getCrossPlatformExecutor() {
        return this.crossPlatformExecutor;
    }

    @Override
    public String toString() {
        return String.format("%s[%x]", this.getClass().getSimpleName(), this.id);
    }

    public Configuration getConfiguration() {
        return this.crossPlatformExecutor.getConfiguration();
    }
}
