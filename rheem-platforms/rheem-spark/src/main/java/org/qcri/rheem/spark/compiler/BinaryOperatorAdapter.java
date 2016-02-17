package org.qcri.rheem.spark.compiler;

import org.apache.spark.api.java.function.Function2;

import java.util.function.BinaryOperator;

/**
 * Wraps a {@link java.util.function.BinaryOperator} as a {@link Function2}.
 */
public class BinaryOperatorAdapter<Type> implements Function2<Type, Type, Type> {

    private BinaryOperator<Type> binaryOperator;

    public BinaryOperatorAdapter(BinaryOperator<Type> binaryOperator) {
        this.binaryOperator = binaryOperator;
    }

    @Override
    public Type call(Type dataQuantum0, Type dataQuantum1) throws Exception {
        return this.binaryOperator.apply(dataQuantum0, dataQuantum1);
    }
}
