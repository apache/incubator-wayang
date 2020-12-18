package io.rheem.rheem.spark.execution;

import org.apache.spark.broadcast.Broadcast;
import io.rheem.rheem.core.api.exception.RheemException;
import io.rheem.rheem.core.function.ExecutionContext;
import io.rheem.rheem.core.plan.rheemplan.InputSlot;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.spark.channels.BroadcastChannel;
import io.rheem.rheem.spark.operators.SparkExecutionOperator;
import io.rheem.rheem.spark.platform.SparkPlatform;

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
