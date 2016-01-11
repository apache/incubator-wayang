package org.qcri.rheem.core.function;

import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataUnitGroupType;

import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

/**
 * This descriptor pertains to functions that take multiple data units and aggregate them into a single data unit
 * by means of a tree-like fold, i.e., using a commutative, associative function..
 */
public class ReduceDescriptor<Type> extends FunctionDescriptor {

    private final DataUnitGroupType inputType;

    private final BasicDataUnitType outputType;

    private final BinaryOperator<Type> javaImplementation;

    public ReduceDescriptor(DataUnitGroupType inputType, BasicDataUnitType outputType,
                            BinaryOperator<Type> javaImplementation) {
        this.inputType = inputType;
        this.outputType = outputType;
        this.javaImplementation = javaImplementation;
    }

    /**
     * This is function is not built to last. It is thought to help out devising programs while we are still figuring
     * out how to express functions in a platform-independent way.
     *
     * @return a function that can perform the reduce
     */
    public BinaryOperator<Type> getJavaImplementation() {
        return this.javaImplementation;
    }


}
