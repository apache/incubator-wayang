package org.qcri.rheem.core.plan.executionplan;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.plan.rheemplan.Slot;
import org.qcri.rheem.core.platform.Platform;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Models the data movement between to {@link ExecutionTask}s.
 */
public abstract class Channel {

    /**
     * Produces the data flowing through this instance.
     */
    protected final ExecutionTask producer;

    /**
     * Consuming {@link ExecutionTask}s of this instance.
     */
    protected final List<ExecutionTask> consumers = new LinkedList<>();

    /**
     * Estimated incurring cardinality of this instance on execution.
     */
    private final CardinalityEstimate cardinalityEstimate;

    /**
     * Mimed instance. Nullable.
     */
    private final Channel original;

    private boolean isMarkedForInstrumentation = false;

    /**
     * Other {@link Channel}s that represent the same {@link OutputSlot}-to-{@link InputSlot} connection from a
     * {@link RheemPlan} and share properties such as {@link #cardinalityEstimate}.
     */
    private Set<Channel> siblings = new HashSet<>(2);


    /**
     * Creates a new, non-hierarchical instance and registers it with the given {@link ExecutionTask}. The
     * {@link CardinalityEstimate} for the instance is retrieved from the {@code producer}.
     *
     * @param producer    produces the data for the instance
     * @param outputIndex index of this instance within the {@code producer}
     */
    protected Channel(ExecutionTask producer, int outputIndex) {
        this(producer, outputIndex, extractCardinalityEstimate(producer, outputIndex));
    }

    /**
     * Creates a new, non-hierarchical instance and registers it with the given {@link ExecutionTask}.
     *
     * @param producer            produces the data for the instance
     * @param outputIndex         index of this instance within the {@code producer}
     * @param cardinalityEstimate a {@link CardinalityEstimate} for this instance
     */
    protected Channel(ExecutionTask producer, int outputIndex, CardinalityEstimate cardinalityEstimate) {
        this.producer = producer;
        this.producer.setOutputChannel(outputIndex, this);
        this.cardinalityEstimate = cardinalityEstimate;
        this.original = null;
    }

    /**
     * Creates a new, hierarchical instance. Mimes the {@code original}'s properties except for the {@link #consumers}.
     *
     * @param original the original instance whose properties will be mimed
     */
    protected Channel(Channel original) {
        this.original = original.getOriginal();
        this.producer = original.getProducer();
        this.cardinalityEstimate = original.getCardinalityEstimate();
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
        if (!this.consumers.contains(consumer)) {
            assert this.isReusable() || this.consumers.isEmpty() :
                    String.format("Cannot add %s as consumer of non-reusable %s, there is already %s.",
                            consumer, this, this.consumers);
            this.consumers.add(consumer);
            consumer.setInputChannel(inputIndex, this);
        }
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
        return this.withSiblings().anyMatch(sibling -> sibling.isMarkedForInstrumentation);

    }

    private Stream<Channel> withSiblings() {
        return Stream.concat(Stream.of(this), this.siblings.stream());
    }

    public void markForInstrumentation() {
        this.withSiblings().forEach(channel -> {
            channel.isMarkedForInstrumentation = true;
            LoggerFactory.getLogger(this.getClass()).debug("Marked {} for instrumentation.", channel);
        });
    }

    @Override
    public String toString() {
        return String.format("%s[%s->%s]", this.getClass().getSimpleName(), this.getProducer(), this.getConsumers());
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

    /**
     * @return an empty {@link Stream} if given {@code null}, otherwise a singleton {@link Stream}
     */
    private static <T> Stream<T> streamNullable(T nullable) {
        return nullable == null ? Stream.empty() : Stream.of(nullable);
    }

    /**
     * Scrap any consumer {@link ExecutionTask}s and sibling {@link Channel}s that are not within the given
     * {@link ExecutionStage}s.
     */
    public void retain(Set<ExecutionStage> retainableStages) {
        this.consumers.removeIf(consumer -> !retainableStages.contains(retainableStages));
        for (Iterator<Channel> i = this.siblings.iterator(); i.hasNext(); ) {
            final Channel sibling = i.next();
            if (!retainableStages.contains(sibling.getProducer().getStage())) {
                i.remove();
                sibling.siblings.remove(this);
            }
        }
    }

    /**
     * Create a copy of this instance. Mimics everything apart from the consumers. Also delimits already executed
     * {@link ExecutionTask}s and those that are not executed yet. Be careful when revising this invariant.
     */
    public abstract Channel copy();

    /**
     * @return if this is not a copy, then this instance, otherwise the root original instance
     */
    public Channel getOriginal() {
        return this.original == null ? this : this.original;
    }

    /**
     * Tells whether this instance is a copy. If so, it delimits already executed
     * {@link ExecutionTask}s and those that are not executed yet. Be careful when revising this invariant.
     *
     * @see #copy()
     * @see #getOriginal()
     */
    public boolean isCopy() {
        return this.original != null;
    }

    /**
     * Merges this instance into the original instance ({@link #getOriginal()}.
     * <p>The consumers of the original instance are cleared and replaced with the consumers of this instance.
     * For all other properties, the original and this instance should agree.</p>
     */
    public void mergeIntoOriginal() {
        if (!this.isCopy()) return;
        this.getOriginal().copyConsumersFrom(this);
    }

    /**
     * Copies the consumers of the given {@code channel} into this instance.
     */
    private void copyConsumersFrom(Channel channel) {
        assert this.consumers.isEmpty();
        for (ExecutionTask consumer : new ArrayList<>(channel.getConsumers())) {
            consumer.exchangeInputChannel(channel, this);
        }
    }
}
