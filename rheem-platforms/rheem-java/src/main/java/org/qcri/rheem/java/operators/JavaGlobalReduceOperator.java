package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.GlobalReduceOperator;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

/**
 * Java implementation of the {@link GlobalReduceOperator}.
 */
public class JavaGlobalReduceOperator<Type>
        extends GlobalReduceOperator<Type>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type             type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public JavaGlobalReduceOperator(DataSetType<Type> type,
                                    ReduceDescriptor<Type> reduceDescriptor) {
        super(type, reduceDescriptor);
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final Stream<Type> inputStream = inputStreams[0];
        final BinaryOperator<Type> reduceFunction = compiler.compile(this.reduceDescriptor);
        final Optional<Type> reduction = inputStream.reduce(reduceFunction);

        return new Stream[]{reduction.isPresent() ? Stream.of(reduction.get()) : Stream.empty()};
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaGlobalReduceOperator<>(getInputType(), getReduceDescriptor());
    }
}
