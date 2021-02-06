/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.core.function;

import org.apache.wayang.core.optimizer.costs.LoadEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.NestableLoadProfileEstimator;
import org.apache.wayang.core.types.BasicDataUnitType;
import org.apache.wayang.core.types.DataUnitGroupType;
import org.apache.wayang.core.types.DataUnitType;

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
