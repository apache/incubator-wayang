package org.qcri.rheem.core.plan.executionplan;

import java.util.LinkedList;
import java.util.List;

/**
 * Models the data movement between to {@link ExecutionTask}s.
 */
public abstract class Channel {

    protected final ExecutionTask producer;

    protected final List<ExecutionTask> consumers = new LinkedList<>();

    protected Channel(ExecutionTask producer) {
        this.producer = producer;
    }

    public abstract boolean isReusable();

}
