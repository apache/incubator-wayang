package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.StdoutSink;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.stream.Stream;

/**
 * Java implementation of the {@link StdoutSink}.
 */
public class JavaStdoutSink<T> extends StdoutSink<T> implements JavaExecutionOperator {

    public JavaStdoutSink(DataSetType type) {
        super(type);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        inputs[0].provideStream().forEach(System.out::println);
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaStdoutSink<>(this.getType());
    }
}
