package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.StdoutSink;

import java.util.stream.Stream;

/**
 * Java implementation of the {@link StdoutSink}.
 */
public class JavaStdoutSink<T> extends StdoutSink<T> implements JavaExecutionOperator {

    public JavaStdoutSink(Class<T> type) {
        super(type);
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams) {
        inputStreams[0].forEach(System.out::println);
        return new Stream[0];
    }
}
