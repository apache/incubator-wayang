package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Java implementation of the {@link FilterOperator}.
 */
public class JavaFilterOperator<Type>
        extends FilterOperator<Type>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public JavaFilterOperator(DataSetType<Type> type, Predicate<Type> predicate) {
        super(type, predicate);
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final Stream<Type> inputStream = inputStreams[0];
        final Stream<Type> outputStream = inputStream.filter(predicate);

        return new Stream[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaFilterOperator<>(getInputType(), getFunctionDescriptor());
    }
}
