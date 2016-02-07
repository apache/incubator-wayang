package org.qcri.rheem.core.plan.executionplan;

import org.apache.commons.lang3.Validate;

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
        consumer.setInputChannel(inputIndex, this);
    }

    public abstract boolean isReusable();

}
