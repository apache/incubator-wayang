package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.stream.Stream;

/**
 * Java implementation of the {@link org.qcri.rheem.basic.operators.FlatMapOperator}.
 */
public class JavaFlatMapOperator<InputType, OutputType>
        extends FlatMapOperator<InputType, OutputType>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param functionDescriptor
     */
    public JavaFlatMapOperator(DataSetType inputType, DataSetType outputType, TransformationDescriptor<InputType, Stream<OutputType>> functionDescriptor) {
        super(inputType, outputType, functionDescriptor);
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final Stream<InputType> inputStream = (Stream<InputType>) inputStreams[0];
        final Stream<OutputType> outputStream = inputStream.flatMap(compiler.compile(this.functionDescriptor));

        return new Stream[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaFlatMapOperator<>(getInputType(), getOutputType(), getFunctionDescriptor());
    }
}
