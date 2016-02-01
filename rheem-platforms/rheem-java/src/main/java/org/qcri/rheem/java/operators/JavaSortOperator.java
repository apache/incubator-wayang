package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.SortOperator;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.stream.Stream;

/**
 * Java implementation of the {@link SortOperator}.
 */
public class JavaSortOperator<Type>
        extends SortOperator<Type>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public JavaSortOperator(DataSetType<Type> type) {
        super(type);
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final Stream<Type> inputStream = inputStreams[0];
        final Stream<Type> outputStream = inputStream.sorted();


        return new Stream[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaSortOperator<>(getInputType());
    }
}
