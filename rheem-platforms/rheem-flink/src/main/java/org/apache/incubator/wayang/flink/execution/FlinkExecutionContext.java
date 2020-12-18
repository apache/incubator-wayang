package io.rheem.rheem.flink.execution;

import org.apache.flink.api.common.functions.RichFunction;
import io.rheem.rheem.core.function.ExecutionContext;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.flink.operators.FlinkExecutionOperator;
import io.rheem.rheem.flink.platform.FlinkPlatform;

import java.io.Serializable;
import java.util.Collection;

/**
 * {@link ExecutionContext} implementation for the {@link FlinkPlatform}.
 */
public class FlinkExecutionContext implements ExecutionContext, Serializable {

    private transient FlinkExecutionOperator operator;

    private transient final ChannelInstance[] inputs;

    private transient int iterationNumber;

    private RichFunction richFunction;


    public FlinkExecutionContext(FlinkExecutionOperator operator, ChannelInstance[] inputs, int iterationNumber) {
        this.operator = operator;
        this.inputs = inputs;
        this.iterationNumber = iterationNumber;
    }


    @Override
    @SuppressWarnings("unchecked")
    public <Type> Collection<Type> getBroadcast(String name) {
        return this.richFunction.getRuntimeContext().getBroadcastVariable(name);
    }

    public void setRichFunction(RichFunction richFunction){
        this.richFunction = richFunction;
    }

    @Override
    public int getCurrentIteration() {
        return this.iterationNumber;
    }
}
