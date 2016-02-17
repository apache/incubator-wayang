package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Implementation of the {@link LocalCallbackSink} operator for the Java platform.
 */
public class JavaLocalCallbackSink<T> extends LocalCallbackSink<T> implements JavaExecutionOperator {
    /**
     * Creates a new instance.
     *
     * @param callback callback that is executed locally for each incoming data unit
     * @param type     type of the incoming elements
     */
    public JavaLocalCallbackSink(Consumer<T> callback, DataSetType type) {
        super(callback, type);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        inputs[0].<T>provideStream().forEach(this.callback);
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaLocalCallbackSink<>(this.callback, this.getType());
    }
}
