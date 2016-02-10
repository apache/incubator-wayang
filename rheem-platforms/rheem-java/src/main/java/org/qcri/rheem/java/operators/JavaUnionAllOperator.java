package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.UnionAllOperator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.stream.Stream;

/**
 * Java implementation of the {@link UnionAllOperator}.
 */
public class JavaUnionAllOperator<Type>
        extends UnionAllOperator<Type>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param type the type of the datasets to be coalesced
     */
    public JavaUnionAllOperator(DataSetType<Type> type) {
        super(type);
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 2) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final Stream<Type> inputStream0 = (Stream<Type>) inputStreams[0];
        final Stream<Type> inputStream1 = (Stream<Type>) inputStreams[1];
        final Stream<Type> outputStream = Stream.concat(inputStream0, inputStream1);

        return new Stream[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaUnionAllOperator<>(this.getInputType0());
    }
}
