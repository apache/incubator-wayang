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

import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.types.BasicDataUnitType;

import java.util.Optional;

/**
 * Created by bertty on 13-07-17.
 */
public class ConsumerDescriptor<T> extends FunctionDescriptor {

    protected final BasicDataUnitType<T> inputType;

    private final SerializableConsumer<T> javaImplementation;

    /**
     * The selectivity ({code 0..1}) of this instance or {@code null} if unspecified.
     */
    private ProbabilisticDoubleInterval selectivity;

    public ConsumerDescriptor(SerializableConsumer<T> javaImplementation, Class<T> inputTypeClass) {
        this(javaImplementation, inputTypeClass, (ProbabilisticDoubleInterval) null);
    }

    public ConsumerDescriptor(SerializableConsumer<T> javaImplementation,
                              Class<T> inputTypeClass,
                              ProbabilisticDoubleInterval selectivity) {
        this(javaImplementation, inputTypeClass, selectivity, null);
    }

    public ConsumerDescriptor(SerializableConsumer<T> javaImplementation,
                              Class<T> inputTypeClass,
                              LoadProfileEstimator loadProfileEstimator) {
        this(javaImplementation, inputTypeClass, null, loadProfileEstimator);
    }

    public ConsumerDescriptor(SerializableConsumer<T> javaImplementation,
                              Class<T> inputTypeClass,
                              ProbabilisticDoubleInterval selectivity,
                              LoadProfileEstimator loadProfileEstimator) {
        this(javaImplementation, BasicDataUnitType.createBasic(inputTypeClass), selectivity, loadProfileEstimator);
    }

    public ConsumerDescriptor(SerializableConsumer<T> javaImplementation,
                              BasicDataUnitType<T> inputType,
                              ProbabilisticDoubleInterval selectivity,
                              LoadProfileEstimator loadProfileEstimator) {
        super(loadProfileEstimator);
        this.javaImplementation = javaImplementation;
        this.inputType = inputType;
        this.selectivity = selectivity;
    }

    /**
     * This function is not built to last. It is thought to help out devising programs while we are still figuring
     * out how to express functions in a platform-independent way.
     *
     * @return a function that can perform the reduce
     */
    public SerializableConsumer<T> getJavaImplementation() {
        return this.javaImplementation;
    }


    /**
     * In generic code, we do not have the type parameter values of operators, functions etc. This method avoids casting issues.
     *
     * @return this instance with type parameters set to {@link Object}
     */
    @SuppressWarnings("unchecked")
    public SerializableConsumer<Object> unchecked() {
        return (SerializableConsumer<Object>) this;
    }

    public BasicDataUnitType<T> getInputType() {
        return this.inputType;
    }

    /**
     * Get the selectivity of this instance.
     *
     * @return an {@link java.util.Optional} with the selectivity or an empty one if no selectivity was specified
     */
    public Optional<ProbabilisticDoubleInterval> getSelectivity() {
        return Optional.ofNullable(this.selectivity);
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.javaImplementation);
    }
}
