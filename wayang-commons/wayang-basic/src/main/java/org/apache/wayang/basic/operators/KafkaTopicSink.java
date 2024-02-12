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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.costs.DefaultLoadEstimator;
import org.apache.wayang.core.optimizer.costs.NestableLoadProfileEstimator;
import org.apache.wayang.core.plan.wayangplan.UnarySink;
import org.apache.wayang.core.types.DataSetType;

import java.util.Objects;

/**
 * This {@link UnarySink} writes all incoming data quanta to a single Kafka topic.
 */
public class KafkaTopicSink<T> extends UnarySink<T> {

    protected final String topicName;

    protected final TransformationDescriptor<T, String> formattingDescriptor;

    private final static Logger logger = LogManager.getLogger(KafkaTopicSink.class);

    /**
     * Creates a new instance with default formatting.
     *
     * @param topicName   Name of Kafka topic that should be written to
     * @param typeClass   {@link Class} of incoming data quanta
     */
    public KafkaTopicSink(String topicName, Class<T> typeClass) {
        this(
                topicName,
                new TransformationDescriptor<>(
                        Objects::toString,
                        typeClass,
                        String.class,
                        new NestableLoadProfileEstimator(
                                new DefaultLoadEstimator(1, 1, .99d, (in, out) -> 10 * in[0]),
                                new DefaultLoadEstimator(1, 1, .99d, (in, out) -> 1000)
                        )
                )
        );
    }


    /**
     * Creates a new instance.
     *
     * @param topicName        Name of Kafka topic that should be written to
     * @param formattingFunction formats incoming data quanta to a {@link String} representation
     * @param typeClass          {@link Class} of incoming data quanta
     */
    public KafkaTopicSink(String topicName,
                          TransformationDescriptor.SerializableFunction<T, String> formattingFunction,
                          Class<T> typeClass) {
        this(
                topicName,
                new TransformationDescriptor<>(formattingFunction, typeClass, String.class)
        );
    }

    /**
     * Creates a new instance.
     *
     * @param topicName          Name of Kafka topic that should be written to
     * @param formattingDescriptor formats incoming data quanta to a {@link String} representation
     */
    public KafkaTopicSink(String topicName, TransformationDescriptor<T, String> formattingDescriptor) {
        super(DataSetType.createDefault(formattingDescriptor.getInputType()));
        this.topicName = topicName;
        this.formattingDescriptor = formattingDescriptor;
    }

    /**
     * Creates a copied instance.
     *
     * @param that should be copied
     */
    public KafkaTopicSink(KafkaTopicSink<T> that) {
        super(that);
        this.topicName = that.topicName;
        this.formattingDescriptor = that.formattingDescriptor;
    }

}
