package org.qcri.rheem.core.plan.executionplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.platform.Platform;

import java.util.LinkedList;
import java.util.List;

/**
 * Models the data movement between to {@link ExecutionTask}s.
 */
public abstract class Channel {

    protected final ExecutionTask producer;

    protected final List<ExecutionTask> consumers = new LinkedList<>();

    protected Channel(ExecutionTask producer, int outputIndex) {
        this.producer = producer;
        this.producer.setOutputChannel(outputIndex, this);
    }

    public void addConsumer(ExecutionTask consumer, int inputIndex) {
        Validate.isTrue(this.isReusable() || this.consumers.isEmpty());
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
}
