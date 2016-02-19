package org.qcri.rheem.core.plan.executionplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.plan.rheemplan.Slot;
import org.qcri.rheem.core.platform.Platform;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Models the data movement between to {@link ExecutionTask}s.
 */
public abstract class Channel {

    protected final ExecutionTask producer;

    protected final List<ExecutionTask> consumers = new LinkedList<>();

    private final CardinalityEstimate cardinalityEstimate;

    private boolean isMarkedForInstrumentation = false;

    /**
     * Other {@link Channel}s that represent the same {@link OutputSlot}-to-{@link InputSlot} connection from a
     * {@link RheemPlan} and share properties such as {@link #cardinalityEstimate}.
     */
    private Set<Channel> siblings = new HashSet<>(2);


    protected Channel(ExecutionTask producer, int outputIndex, CardinalityEstimate cardinalityEstimate) {
        this.producer = producer;
        this.producer.setOutputChannel(outputIndex, this);
        this.cardinalityEstimate = cardinalityEstimate;
    }

    protected Channel(ExecutionTask producer, int outputIndex) {
        this(producer, outputIndex, extractCardinalityEstimate(producer, outputIndex));
    }

    public static CardinalityEstimate extractCardinalityEstimate(ExecutionTask task, int outputIndex) {
        return task.getOperator().getOutput(outputIndex).getCardinalityEstimate();
    }

    /**
     * Set up a consumer {@link ExecutionTask} for this instance.
     *
     * @param consumer   the new consumer
     * @param inputIndex the input index for this instance into the consumer
     */
    public void addConsumer(ExecutionTask consumer, int inputIndex) {
        Validate.isTrue(this.isReusable() || this.consumers.isEmpty(),
                "Cannot add %s as consumer of non-reusable %s, there is already %s.",
                consumer, this, this.consumers);
        this.consumers.add(consumer);
        consumer.setInputChannel(inputIndex, this);
    }

    /**
     * Declares whether this is not a read-once instance.
     *
     * @return whether this instance can have multiple consumers
     */
    public abstract boolean isReusable();

    /**
     * Tells whether this instance connects different {@link PlatformExecution}s. The answer is not necessarily
     * only determined by whether this instance connects {@link ExecutionTask}s of different {@link Platform}s.
     *
     * @return whether this instance connects different {@link PlatformExecution}s
     */
    public boolean isExecutionBreaker() {
        // Default implementation: Check if all producers/consumers are from the same Platform.
        Platform singlePlatform = null;
        if (this.producer != null) {
            singlePlatform = this.producer.getOperator().getPlatform();
        }

        for (ExecutionTask consumer : this.consumers) {
            if (singlePlatform == null) {
                singlePlatform = consumer.getOperator().getPlatform();
            } else if (!singlePlatform.equals(consumer.getOperator().getPlatform())) {
                return true;
            }
        }

        return false;
    }

    public ExecutionTask getProducer() {
        return this.producer;
    }

    public List<ExecutionTask> getConsumers() {
        return this.consumers;
    }

    public CardinalityEstimate getCardinalityEstimate() {
        return this.cardinalityEstimate;
    }

    public boolean isMarkedForInstrumentation() {
        return withSiblings()
                .anyMatch(sibling -> sibling.isMarkedForInstrumentation);

    }

    private Stream<Channel> withSiblings() {
        return Stream.concat(Stream.of(this), this.siblings.stream());
    }

    public void markForInstrumentation() {
        this.isMarkedForInstrumentation = true;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    /**
     * Acquaints the given instance with this instance and all existing {@link #siblings}.
     */
    public void addSibling(Channel sibling) {
        this.withSiblings().forEach(olderSibling -> olderSibling.relateTo(sibling));
    }

    /**
     * Makes this and the given instance siblings.
     */
    private void relateTo(Channel sibling) {
        this.siblings.add(sibling);
        sibling.siblings.add(this);
    }

    /**
     * @return all {@link InputSlot}s and {@link OutputSlot}s that are represented by this instance and its
     * {@link #siblings}
     */
    public Collection<Slot<?>> getCorrespondingSlots() {
        return this.withSiblings()
                .map(Channel::getCorrespondingSlotsLocal)
                .reduce(Stream.empty(), Stream::concat)
                .collect(Collectors.toList());
    }

    private Stream<Slot<?>> getCorrespondingSlotsLocal() {
        final Stream<? extends OutputSlot<?>> outputSlotStream =
                streamNullable(this.getProducer().getOutputSlotFor(this));
        final Stream<? extends InputSlot<?>> inputSlotStream
                = this.consumers.stream().flatMap(consumer -> streamNullable(consumer.getInputSlotFor(this)));
        return Stream.concat(inputSlotStream, outputSlotStream);
    }

    private static <T> Stream<T> streamNullable(T nullable) {
        return nullable == null ? Stream.empty() : Stream.of(nullable);
    }
}
