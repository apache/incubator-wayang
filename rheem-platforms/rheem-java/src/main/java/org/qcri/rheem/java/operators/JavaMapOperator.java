package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.function.MapFunctionDescriptor;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.core.types.DataSet;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.stream.Stream;

/**
 * Java implementation of the {@link org.qcri.rheem.basic.operators.MapOperator}.
 */
public class JavaMapOperator<InputType, OutputType>
        extends MapOperator<InputType, OutputType>
        implements JavaExecutionOperator {

    /**
     * Function carried by this operator.
     */
    private final MapFunctionDescriptor<InputType, OutputType> functionDescriptor;

    /**
     * Creates a new instance.
     *
     * @param functionDescriptor
     */
    public JavaMapOperator(DataSet inputType, DataSet outputType, MapFunctionDescriptor<InputType, OutputType> functionDescriptor) {
        super(inputType, outputType, functionDescriptor);
        this.functionDescriptor = functionDescriptor;
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final Stream<InputType> inputStream = (Stream<InputType>) inputStreams[0];
        final Stream<OutputType> outputStream = inputStream.map(compiler.compile(this.functionDescriptor));

        return new Stream[]{outputStream};
    }
}
