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

import java.util.function.Function;
import org.apache.wayang.core.optimizer.costs.LoadEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.NestableLoadProfileEstimator;
import org.apache.wayang.core.types.BasicDataUnitType;

/**
 * This descriptor pertains to functions that consume a single data unit and output a single data unit.
 *
 * @param <Input>  input type of the transformation function
 * @param <Output> output type of the transformation function
 */
public class TransformationDescriptor<Input, Output> extends FunctionDescriptor {

    protected final BasicDataUnitType<Input> inputType;

    protected final BasicDataUnitType<Output> outputType;

    private final FunctionDescriptor.SerializableFunction<Input, Output> javaImplementation;

    public TransformationDescriptor(FunctionDescriptor.SerializableFunction<Input, Output> javaImplementation,
                                    Class<Input> inputTypeClass,
                                    Class<Output> outputTypeClass) {
        this(javaImplementation,
                BasicDataUnitType.createBasic(inputTypeClass),
                BasicDataUnitType.createBasic(outputTypeClass));
    }

    public TransformationDescriptor(FunctionDescriptor.SerializableFunction<Input, Output> javaImplementation,
                                    Class<Input> inputTypeClass,
                                    Class<Output> outputTypeClass,
                                    LoadProfileEstimator loadProfileEstimator) {
        this(javaImplementation,
                BasicDataUnitType.createBasic(inputTypeClass),
                BasicDataUnitType.createBasic(outputTypeClass),
                loadProfileEstimator);
    }

    public TransformationDescriptor(FunctionDescriptor.SerializableFunction<Input, Output> javaImplementation,
                                    BasicDataUnitType<Input> inputType,
                                    BasicDataUnitType<Output> outputType) {
        this(javaImplementation, inputType, outputType,
                new NestableLoadProfileEstimator(
                        LoadEstimator.createFallback(1, 1),
                        LoadEstimator.createFallback(1, 1)
                )
        );
    }

    public TransformationDescriptor(FunctionDescriptor.SerializableFunction<Input, Output> javaImplementation,
                                    BasicDataUnitType<Input> inputType,
                                    BasicDataUnitType<Output> outputType,
                                    LoadProfileEstimator loadProfileEstimator) {
        super(loadProfileEstimator);
        this.javaImplementation = javaImplementation;
        this.inputType = inputType;
        this.outputType = outputType;
    }

    /**
     * This is function is not built to last. It is thought to help out devising programs while we are still figuring
     * out how to express functions in a platform-independent way.
     *
     * @return a function that can perform the reduce
     */
    public Function<Input, Output> getJavaImplementation() {
        return this.javaImplementation;
    }

    /**
     * In generic code, we do not have the type parameter values of operators, functions etc. This method avoids casting issues.
     *
     * @return this instance with type parameters set to {@link Object}
     */
    @SuppressWarnings("unchecked")
    public TransformationDescriptor<Object, Object> unchecked() {
        return (TransformationDescriptor<Object, Object>) this;
    }

    public BasicDataUnitType<Input> getInputType() {
        return this.inputType;
    }

    public BasicDataUnitType<Output> getOutputType() {
        return this.outputType;
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.javaImplementation);
    }
}
