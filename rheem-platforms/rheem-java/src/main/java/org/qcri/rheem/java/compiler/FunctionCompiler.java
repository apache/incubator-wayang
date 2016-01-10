package org.qcri.rheem.java.compiler;

import org.qcri.rheem.basic.function.MapFunctionDescriptor;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * A compiler translates Rheem functions into executable Java functions.
 */
public class FunctionCompiler {

    private final Map<Class<? extends MapFunctionDescriptor<?, ?>>, Function<?, ?>> implementations
            = new HashMap<>();

    public <InputType, OutputType> void registerMapFunction(
            Class<? extends MapFunctionDescriptor<InputType, OutputType>> mapFunctionDescriptorClass,
            Function<InputType, OutputType> implementation) {
        this.implementations.put(mapFunctionDescriptorClass, implementation);
    }

    public <InputType, OutputType> Function<InputType, OutputType> compile(
            MapFunctionDescriptor<InputType, OutputType> functionDescriptor) {
        final Function<?, ?> function = this.implementations.get(functionDescriptor.getClass());
        if (function == null) {
            throw new IllegalArgumentException("Cannot compile the given descriptor.");
        }
        return (Function<InputType, OutputType>) function;
    }

}
