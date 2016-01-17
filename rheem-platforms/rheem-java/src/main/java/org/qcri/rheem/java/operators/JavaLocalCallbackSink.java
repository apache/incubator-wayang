package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
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
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        final Stream<T> inputStream = inputStreams[0];
        inputStream.forEach(this.callback);
        return new Stream[0];
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaLocalCallbackSink<>(this.callback, getType());
    }
}
