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

package org.apache.wayang.basic.operators;

import org.apache.commons.lang3.Validate;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.ConsumerDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.UnarySink;
import org.apache.wayang.core.types.BasicDataUnitType;
import org.apache.wayang.core.types.DataSetType;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * This sink executes a callback on each received data unit into a Java {@link Collection}.
 */
public class LocalCallbackSink<T> extends UnarySink<T> {

    protected final Consumer<T> callback;

    protected final ConsumerDescriptor.SerializableConsumer<T> callbackDescriptor;

    protected Collection<T> collector;

    public static <T> LocalCallbackSink<T> createCollectingSink(Collection<T> collector, DataSetType<T> type) {
        return new LocalCallbackSink<>((ConsumerDescriptor.SerializableConsumer<T>)collector::add, type).setCollector(collector);
    }

    public static <T> LocalCallbackSink<T> createCollectingSink(Collection<T> collector, Class<T> typeClass) {
        return new LocalCallbackSink<>((ConsumerDescriptor.SerializableConsumer<T>)collector::add, typeClass).setCollector(collector);
    }

    public static <T> LocalCallbackSink<T> createStdoutSink(DataSetType<T> type) {
        return new LocalCallbackSink<>((ConsumerDescriptor.SerializableConsumer<T>)System.out::println, type);
    }

    public static <T> LocalCallbackSink<T> createStdoutSink(Class<T> typeClass) {
        return new LocalCallbackSink<>((ConsumerDescriptor.SerializableConsumer<T>)System.out::println, typeClass);
    }

    /**
     * Provides a sink that swallows items and does nothing
     */
    public static <T> LocalCallbackSink<T> createNoOutSink(DataSetType<T> type) {
        return new LocalCallbackSink<>((ConsumerDescriptor.SerializableConsumer<T>)item -> {}, type);
    }

    /**
     * Provides a sink that swallows items and does nothing
     */
    public static <T> LocalCallbackSink<T> createNoOutSink(Class<T> typeClass) {
        return new LocalCallbackSink<>((ConsumerDescriptor.SerializableConsumer<T>)item -> {}, typeClass);
    }


    /**
     * Creates a new instance.
     *
     * @param callback callback that is executed locally for each incoming data unit
     * @param type     type of the incoming elements
     */
    public LocalCallbackSink(Consumer<T> callback, DataSetType<T> type) {
        super(type);
        this.callback = callback;
        this.callbackDescriptor = null;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public LocalCallbackSink(LocalCallbackSink<T> that) {
        super(that);
        this.callback = that.getCallback();
        this.callbackDescriptor = that.getCallbackDescriptor();
        this.collector = that.collector;
    }


    /**
     * Creates a new instance.
     *
     * @param callback callback that is executed locally for each incoming data unit
     * @param typeClass     type of the incoming elements
     */
    public LocalCallbackSink(Consumer<T> callback, Class<T> typeClass) {
        this(callback, DataSetType.createDefault(typeClass));
    }

    /**
     * Creates a new instance.
     */
    public LocalCallbackSink(ConsumerDescriptor.SerializableConsumer<T> consumerDescriptor, Class<T> typeClass) {
        super(DataSetType.createDefault(BasicDataUnitType.createBasic(typeClass)), true);
        this.callbackDescriptor = consumerDescriptor;
        this.callback = consumerDescriptor;
    }

    /**
     * Creates a new instance.
     *
     * @param type type of the dataunit elements
     */
    public LocalCallbackSink(ConsumerDescriptor.SerializableConsumer<T> consumerDescriptor, DataSetType<T> type) {
        super(type, true);
        this.callbackDescriptor = consumerDescriptor;
        this.callback = consumerDescriptor;
    }

    public LocalCallbackSink<T> setCollector(Collection<T> collector){
        this.collector = collector;
        return this;
    }

    /**
     *  Convnience constructor, defaults to StdoutSink
     */
    public LocalCallbackSink(Class<T> typeClass ){
        this((FunctionDescriptor.SerializableConsumer<T>)System.out::println, typeClass);
    }

    public Consumer<T> getCallback() {
        return this.callback;
    }

    public ConsumerDescriptor.SerializableConsumer<T> getCallbackDescriptor(){
        return this.callbackDescriptor;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return super.createCardinalityEstimator(outputIndex, configuration);
    }
}
