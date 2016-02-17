package org.qcri.rheem.java.execution;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.Collection;

/**
 * {@link ExecutionContext} implementation for the {@link JavaPlatform}.
 */
public class JavaExecutionContext implements ExecutionContext {

    private final JavaExecutionOperator operator;

    private final ChannelExecutor[] inputs;

    public JavaExecutionContext(JavaExecutionOperator operator, ChannelExecutor[] inputs) {
        this.operator = operator;
        this.inputs = inputs;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Collection<T> getBroadcast(String name) {
        for (int i = 0; i < this.operator.getNumInputs(); i++) {
            final InputSlot<?> input = this.operator.getInput(i);
            if (input.isBroadcast() && input.getName().equals(name)) {
                final ChannelExecutor channelExecutor = this.inputs[i];
                assert channelExecutor.canProvideCollection();
                return (Collection<T>) channelExecutor.provideCollection();
            }
        }

        throw new RheemException("No such broadcast found: " + name);
    }
}
