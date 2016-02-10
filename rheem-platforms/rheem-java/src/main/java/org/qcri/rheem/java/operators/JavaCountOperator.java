package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.CountOperator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.stream.Stream;

/**
 * Java implementation of the {@link CountOperator}.
 */
public class JavaCountOperator<Type>
        extends CountOperator<Type>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public JavaCountOperator(DataSetType<Type> type) {
        super(type);
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final Stream<Type> inputStream = inputStreams[0];
        final Long count = inputStream.count();

        return new Stream[]{Stream.of(count)};
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaCountOperator<>(this.getInputType());
    }
}
