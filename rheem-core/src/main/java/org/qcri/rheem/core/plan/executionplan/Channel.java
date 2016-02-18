package org.qcri.rheem.core.plan.executionplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.platform.Platform;

import java.util.LinkedList;
import java.util.List;

/**
 * Models the data movement between to {@link ExecutionTask}s.
 */
public abstract class Channel {

    protected final ExecutionTask producer;

    protected final List<ExecutionTask> consumers = new LinkedList<>();

    private final CardinalityEstimate cardinalityEstimate;

    private boolean isMarkedForInstrumentation = false;

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
        return this.isMarkedForInstrumentation;
    }

    public void markForInstrumentation() {
        this.isMarkedForInstrumentation = true;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
