package org.qcri.rheem.core.function;

import org.qcri.rheem.core.optimizer.costs.ResourceFunction;
import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataUnitGroupType;
import org.qcri.rheem.core.types.DataUnitType;

import java.util.function.BinaryOperator;

/**
 * This descriptor pertains to functions that take multiple data units and aggregate them into a single data unit
 * by means of a tree-like fold, i.e., using a commutative, associative function..
 */
public class ReduceDescriptor<Type> extends FunctionDescriptor {

    private final DataUnitGroupType<Type> inputType;

    private final BasicDataUnitType<Type> outputType;

    private final BinaryOperator<Type> javaImplementation;

    public ReduceDescriptor(DataUnitGroupType<Type> inputType, BasicDataUnitType<Type> outputType,
                            BinaryOperator<Type> javaImplementation) {
        this(inputType, outputType, javaImplementation,
                ResourceFunction.createFallback(1, 1),
                ResourceFunction.createFallback(1, 1));
    }

    public ReduceDescriptor(DataUnitGroupType<Type> inputType, BasicDataUnitType<Type> outputType,
                            BinaryOperator<Type> javaImplementation, ResourceFunction cpuResourceFunction,
                            ResourceFunction memoryResourceFunction) {
        super(cpuResourceFunction, memoryResourceFunction);
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

    /**
     * In generic code, we do not have the type parameter values of operators, functions etc. This method avoids casting issues.
     *
     * @return this instance with type parameters set to {@link Object}
     */
    @SuppressWarnings("unchecked")
    public ReduceDescriptor<Object> unchecked() {
        return (ReduceDescriptor<Object>) this;
    }

}
