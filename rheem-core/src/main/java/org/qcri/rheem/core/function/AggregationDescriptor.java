package org.qcri.rheem.core.function;

import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataUnitGroupType;
import org.qcri.rheem.core.types.DataUnitType;

import java.util.Iterator;

/**
 * This descriptor pertains to functions that take multiple data units and aggregate them into a single data unit.
 */
public abstract class AggregationDescriptor<InputType, OutputType> extends FunctionDescriptor {

    private final DataUnitGroupType<InputType> inputType;

    private final BasicDataUnitType<OutputType> outputType;

    // TODO: What about aggregation functions?

    public AggregationDescriptor(DataUnitGroupType<InputType> inputType, BasicDataUnitType<OutputType> outputType) {
        this(inputType, outputType, new NestableLoadProfileEstimator(
                LoadEstimator.createFallback(1, 1),
                LoadEstimator.createFallback(1, 1)
        ));
    }

    public AggregationDescriptor(Class<InputType> inputTypeClass, Class<OutputType> outputTypeClass) {
        this(inputTypeClass, outputTypeClass, new NestableLoadProfileEstimator(
                LoadEstimator.createFallback(1, 1),
                LoadEstimator.createFallback(1, 1)
        ));
    }

    public AggregationDescriptor(Class<InputType> inputTypeClass,
                                 Class<OutputType> outputTypeClass,
                                 LoadProfileEstimator loadProfileEstimator) {
        this(DataUnitType.createGrouped(inputTypeClass),
                DataUnitType.createBasic(outputTypeClass),
                loadProfileEstimator);
    }

    public AggregationDescriptor(DataUnitGroupType<InputType> inputType, BasicDataUnitType<OutputType> outputType,
                                 LoadProfileEstimator loadProfileEstimator) {
        super(loadProfileEstimator);
        this.inputType = inputType;
        this.outputType = outputType;
    }

    /**
     * This is function is not built to last. It is thought to help out devising programs while we are still figuring
     * out how to express functions in a platform-independent way.
     *
     * @param <Input>  input type of the function
     * @param <Output> output type of the function
     * @return a function that can perform the reduce
     */
    public abstract <Input, Output> FlatMapDescriptor.SerializableFunction<Iterator<Input>, Output> getJavaImplementation();

    /**
     * In generic code, we do not have the type parameter values of operators, functions etc. This method avoids casting issues.
     *
     * @return this instance with type parameters set to {@link Object}
     */
    @SuppressWarnings("unchecked")
    public AggregationDescriptor<Object, Object> unchecked() {
        return (AggregationDescriptor<Object, Object>) this;
    }

    public DataUnitGroupType<InputType> getInputType() {
        return this.inputType;
    }

    public BasicDataUnitType<OutputType> getOutputType() {
        return this.outputType;
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.getJavaImplementation());
    }
}
