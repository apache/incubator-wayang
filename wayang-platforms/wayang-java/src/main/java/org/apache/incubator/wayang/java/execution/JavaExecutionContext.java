package org.apache.incubator.wayang.java.execution;

import org.apache.incubator.wayang.core.api.exception.WayangException;
import org.apache.incubator.wayang.core.function.ExecutionContext;
import org.apache.incubator.wayang.core.plan.wayangplan.InputSlot;
import org.apache.incubator.wayang.core.platform.ChannelInstance;
import org.apache.incubator.wayang.java.channels.CollectionChannel;
import org.apache.incubator.wayang.java.operators.JavaExecutionOperator;
import org.apache.incubator.wayang.java.platform.JavaPlatform;

import java.util.Collection;

/**
 * {@link ExecutionContext} implementation for the {@link JavaPlatform}.
 */
public class JavaExecutionContext implements ExecutionContext {

    private final JavaExecutionOperator operator;

    private final ChannelInstance[] inputs;

    private final int iterationNumber;

    public JavaExecutionContext(JavaExecutionOperator operator, ChannelInstance[] inputs, int iterationNumber) {
        this.operator = operator;
        this.inputs = inputs;
        this.iterationNumber = iterationNumber;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Collection<T> getBroadcast(String name) {
        for (int i = 0; i < this.operator.getNumInputs(); i++) {
            final InputSlot<?> input = this.operator.getInput(i);
            if (input.isBroadcast() && input.getName().equals(name)) {
                final CollectionChannel.Instance broadcastChannelInstance = (CollectionChannel.Instance) this.inputs[i];
                return (Collection<T>) broadcastChannelInstance.provideCollection();
            }
        }

        throw new WayangException("No such broadcast found: " + name);
    }

    @Override
    public int getCurrentIteration() {
        return this.iterationNumber;
    }
}
