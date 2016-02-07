package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.CoalesceOperator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.stream.Stream;

/**
 * {@link JavaExecutionOperator} for the {@link CoalesceOperator}.
 */
public class JavaCoalesceOperator<Type> extends CoalesceOperator<Type> implements JavaExecutionOperator {
    /**
     * Creates a new instance.
     *
     * @param type      the type of the datasets to be coalesced
     */
    public JavaCoalesceOperator(DataSetType<Type> type) {
        super(type);
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 2) {
            throw new IllegalArgumentException("Illegal number of input streams.");
        }
        return new Stream[]{Stream.concat(inputStreams[0], inputStreams[1])};
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaCoalesceOperator<>(this.getOutput().getType());
    }
}
