package org.qcri.rheem.core.function;

import java.io.Serializable;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * A function operates on single data units or collections of those.
 */
public abstract class FunctionDescriptor {

    @FunctionalInterface
    public interface SerializableFunction<Input, Output> extends Function<Input, Output>, Serializable {
    }

    @FunctionalInterface
    public interface SerializableBinaryOperator<Type> extends BinaryOperator<Type>, Serializable {
    }
}
