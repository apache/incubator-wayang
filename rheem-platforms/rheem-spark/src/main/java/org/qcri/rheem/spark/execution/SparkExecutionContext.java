package org.qcri.rheem.spark.execution;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;

/**
 * {@link ExecutionContext} implementation for the {@link SparkPlatform}.
 */
public class SparkExecutionContext implements ExecutionContext {

    private final SparkExecutionOperator operator;

    private final ChannelExecutor[] inputs;

    public SparkExecutionContext(SparkExecutionOperator operator, ChannelExecutor[] inputs) {
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
                return (Collection<T>) channelExecutor.provideBroadcast().getValue();
            }
        }

        throw new RheemException("No such broadcast found: " + name);
    }
}
