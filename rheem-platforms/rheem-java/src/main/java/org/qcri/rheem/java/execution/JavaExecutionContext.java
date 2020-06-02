package org.qcri.rheem.java.execution;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.operators.JavaExecutionOperator;
import org.qcri.rheem.java.platform.JavaPlatform;

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

        throw new RheemException("No such broadcast found: " + name);
    }

    @Override
    public int getCurrentIteration() {
        return this.iterationNumber;
    }
}
