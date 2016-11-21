package org.qcri.rheem.core.function;

import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
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

    private final SerializableBinaryOperator<Type> javaImplementation;

    public ReduceDescriptor(SerializableBinaryOperator<Type> javaImplementation,
                            DataUnitGroupType<Type> inputType,
                            BasicDataUnitType<Type> outputType) {
        this(javaImplementation, inputType, outputType, new NestableLoadProfileEstimator(
                LoadEstimator.createFallback(1, 1),
                LoadEstimator.createFallback(1, 1)
        ));
    }

    public ReduceDescriptor(SerializableBinaryOperator<Type> javaImplementation,
                            DataUnitGroupType<Type> inputType, BasicDataUnitType<Type> outputType,
                            LoadProfileEstimator loadProfileEstimator) {
        super(loadProfileEstimator);
        this.inputType = inputType;
        this.outputType = outputType;
        this.javaImplementation = javaImplementation;
    }

    public ReduceDescriptor(SerializableBinaryOperator<Type> javaImplementation, Class<Type> inputType) {
        this(javaImplementation, DataUnitType.createGroupedUnchecked(inputType),
                DataUnitType.createBasicUnchecked(inputType)
        );
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

    public DataUnitGroupType<Type> getInputType() {
        return this.inputType;
    }

    public BasicDataUnitType<Type> getOutputType() {
        return this.outputType;
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.javaImplementation);
    }
}
