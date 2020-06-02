package org.qcri.rheem.flink.execution;

import org.apache.flink.api.common.functions.RichFunction;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.flink.operators.FlinkExecutionOperator;
import org.qcri.rheem.flink.platform.FlinkPlatform;

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
