package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.function.MapFunctionDescriptor;
import org.qcri.rheem.basic.operators.MapOperator;
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
     * Create a new operator an avoid the type checking.
     * @see JavaMapOperator#JavaMapOperator(Class, Class, MapFunctionDescriptor)
     */
    public static JavaMapOperator<?, ?> createUnchecked(Class<?> inputTypeClass,
                                                         Class<?> outputTypeClass,
                                                         MapFunctionDescriptor<?, ?> functionDescriptor) {
        return new JavaMapOperator<>((Class<Object>) inputTypeClass,
                (Class<Object>) outputTypeClass,
                (MapFunctionDescriptor<Object, Object>) functionDescriptor);
    }

    /**
     * Creates a new instance.
     *
     * @param inputTypeClass     class of the input types (i.e., type of {@link #getInput()}
     * @param outputTypeClass    class of the output types (i.e., type of {@link #getOutput()}
     * @param functionDescriptor
     */
    public JavaMapOperator(Class<InputType> inputTypeClass, Class<OutputType> outputTypeClass, MapFunctionDescriptor<InputType, OutputType> functionDescriptor) {
        super(inputTypeClass, outputTypeClass, functionDescriptor);
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
