package org.qcri.rheem.core.function;

import org.qcri.rheem.core.types.BasicDataUnitType;

import java.util.function.Function;

/**
 * This descriptor pertains to functions that consume a single data unit and output a single data unit.
 *
 * @param <Input>  input type of the transformation function
 * @param <Output> output type of the transformation function
 */
public class TransformationDescriptor<Input, Output> extends FunctionDescriptor {

    protected final BasicDataUnitType inputType;

    protected final BasicDataUnitType outputType;

    private final Function<Input,Output> javaImplementation;

    public TransformationDescriptor(Function<Input, Output> javaImplementation,
                                       BasicDataUnitType inputType,
                                       BasicDataUnitType outputType) {
        this.javaImplementation = javaImplementation;
        this.inputType = inputType;
        this.outputType = outputType;
    }

    /**
     * This is function is not built to last. It is thought to help out devising programs while we are still figuring
     * out how to express functions in a platform-independent way.
     *
     * @return a function that can perform the reduce
     */
    public Function<Input, Output> getJavaImplementation() {
        return this.javaImplementation;
    }

    /**
     * In generic code, we do not have the type parameter values of operators, functions etc. This method avoids casting issues.
     *
     * @return this instance with type parameters set to {@link Object}
     */
    @SuppressWarnings("unchecked")
    public TransformationDescriptor<Object, Object> unchecked() {
        return (TransformationDescriptor<Object, Object>) this;
    }
}
