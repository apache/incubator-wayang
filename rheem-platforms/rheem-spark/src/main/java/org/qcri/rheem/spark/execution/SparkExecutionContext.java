package org.qcri.rheem.spark.execution;

import org.apache.spark.broadcast.Broadcast;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.spark.channels.BroadcastChannel;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link ExecutionContext} implementation for the {@link SparkPlatform}.
 */
public class SparkExecutionContext implements ExecutionContext, Serializable {

    /**
     * Iteration number of the execution.
     */
    private int iterationNumber;

    /**
     * Mapping of broadcast name to {@link Broadcast} references.
     */
    private Map<String, Broadcast<?>> broadcasts;

    /**
     * Creates a new instance.
     *
     * @param operator {@link SparkExecutionOperator} for that the instance should be created
     * @param inputs   {@link ChannelInstance} inputs for the {@code operator}
     */
    public SparkExecutionContext(SparkExecutionOperator operator, ChannelInstance[] inputs, int iterationNumber) {
        this.broadcasts = new HashMap<>();
        for (int inputIndex = 0; inputIndex < operator.getNumInputs(); inputIndex++) {
            InputSlot<?> inputSlot = operator.getInput(inputIndex);
            if (inputSlot.isBroadcast()) {
                final BroadcastChannel.Instance broadcastChannelInstance = (BroadcastChannel.Instance) inputs[inputIndex];
                this.broadcasts.put(inputSlot.getName(), broadcastChannelInstance.provideBroadcast());
            }
        }
        this.iterationNumber = iterationNumber;
    }

    /**
     * Creates a new instance.
     */
    public SparkExecutionContext(int iterationNumber) {
        this.iterationNumber = iterationNumber;
    }

    /**
     * For serialization purposes.
     */
    @SuppressWarnings("unused")
    private SparkExecutionContext() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Collection<T> getBroadcast(String name) {
        final Broadcast<?> broadcast = this.broadcasts.get(name);
        if (broadcast == null) {
            throw new RheemException("No such broadcast found: " + name);
        }

        return (Collection<T>) broadcast.getValue();
    }

    @Override
    public int getCurrentIteration() {
        return this.iterationNumber;
    }
}
