package org.qcri.rheem.core.function;

import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataUnitGroupType;

import java.util.Iterator;
import java.util.function.Function;

/**
 * This descriptor pertains to functions that take multiple data units and aggregate them into a single data unit.
 */
public abstract class AggregationDescriptor extends FunctionDescriptor {

    private final DataUnitGroupType inputType;

    private final BasicDataUnitType outputType;

    public AggregationDescriptor(DataUnitGroupType inputType, BasicDataUnitType outputType) {
        this.inputType = inputType;
        this.outputType = outputType;
    }

    /**
     * This is function is not built to last. It is thought to help out devising programs while we are still figuring
     * out how to express functions in a platform-independent way.
     *
     * @param <Input> input type of the function
     * @param <Output> output type of the function
     * @return a function that can perform the reduce
     */
    public abstract <Input, Output> Function<Iterator<Input>, Output> getJavaImplementation();
}
