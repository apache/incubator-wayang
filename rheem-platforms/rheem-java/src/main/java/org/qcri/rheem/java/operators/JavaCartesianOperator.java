package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.CartesianOperator;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.List;
import java.util.function.Supplier;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Java implementation of the {@link CartesianOperator}.
 */
public class JavaCartesianOperator<InputType0, InputType1>
        extends CartesianOperator<InputType0, InputType1>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     *
     */
    public JavaCartesianOperator(DataSetType<InputType0> inputType0, DataSetType<InputType1> inputType1) {
        super(inputType0, inputType1);
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 2) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }
        final Stream<InputType0> inputStream0 = (Stream<InputType0>) inputStreams[0];
        final Stream<InputType1> inputStream1 = (Stream<InputType1>) inputStreams[1];

        // Hack to avoid consuming stream more than once, collect it as a list.
        // TODO remove this hack
        final List<InputType1> input1 = inputStream1.collect(Collectors.toList());
        final Stream outputStream =
                inputStream0.flatMap(e1 -> input1.stream().map(e2 -> new Tuple2<>(e1, e2)));

        return new Stream[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaCartesianOperator<>(getInputType0(), getInputType1());
    }
}
