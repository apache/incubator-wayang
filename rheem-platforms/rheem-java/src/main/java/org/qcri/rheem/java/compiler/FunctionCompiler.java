package org.qcri.rheem.java.compiler;

import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;

import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * A compiler translates Rheem functions into executable Java functions.
 */
public class FunctionCompiler {

    /**
     * Compile a transformation.
     *
     * @param descriptor describes the transformation
     * @param <I>        input type of the transformation
     * @param <O>        output type of the transformation
     * @return a compiled function
     */
    public <I, O> Function<I, O> compile(TransformationDescriptor<I, O> descriptor) {
        // This is a dummy method but shows the intention of having something compilable in the descriptors.
        return descriptor.getJavaImplementation();
    }

    /**
     * Compile a reduction.
     *
     * @param descriptor describes the transformation
     * @param <Type>        input/output type of the transformation
     * @return a compiled function
     */
    public <Type> BinaryOperator<Type> compile(ReduceDescriptor descriptor) {
        // This is a dummy method but shows the intention of having something compilable in the descriptors.
        return descriptor.getJavaImplementation();
    }
}
