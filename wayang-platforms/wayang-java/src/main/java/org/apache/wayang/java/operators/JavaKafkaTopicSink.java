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

package org.apache.wayang.java.operators;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.wayang.basic.operators.KafkaTopicSink;
import org.apache.wayang.basic.operators.KafkaTopicSource;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;

import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation fo the {@link KafkaTopicSink} for the {@link JavaPlatform}.
 */
public class JavaKafkaTopicSink<T> extends KafkaTopicSink<T> implements JavaExecutionOperator {

    private static final Logger logger = LoggerFactory.getLogger(JavaKafkaTopicSink.class);

    public JavaKafkaTopicSink(String topicName, TransformationDescriptor<T, String> formattingDescriptor) {
        super(topicName, formattingDescriptor);
        logger.info("---> CREATE JavaKafkaTopicSink ... (Option 2)");
    }

    public JavaKafkaTopicSink(KafkaTopicSink<T> that) {
        super(that);
        logger.info("---> CREATE JavaKafkaTopicSink ... (Option 1)");
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == 1;
        assert outputs.length == 0;

        logger.info("---> WRITE TO KAFKA SINK...");

        logger.info("### 9 ... ");

        JavaChannelInstance input = (JavaChannelInstance) inputs[0];

        initProducer( (KafkaTopicSink<T>) this );

        final Function<T, String> formatter = javaExecutor.getCompiler().compile(this.formattingDescriptor);

        logger.info("### 10 ... ");

        try ( KafkaProducer<String,String> producer = getProducer() ) {
            input.<T>provideStream().forEach(
                    dataQuantum -> {
                        try {
                            String messageValue = formatter.apply(dataQuantum);
                            System.out.println(messageValue);

                            // Assuming you have a topic name
                            String topicName = this.topicName;

                            // Create a ProducerRecord. You can also specify a key as the second parameter if needed
                            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, messageValue);

                            // Send the record to the topic
                            producer.send(record, (metadata, exception) -> {
                                if (exception != null) {
                                    // Handle any exceptions thrown during send
                                    logger.error("Failed to send message: " + exception.getMessage());
                                } else {
                                    // Optionally handle successful send, log metadata, etc.
                                    logger.info("Message sent successfully to " + metadata.topic() + " partition " + metadata.partition());
                                }
                            });
                        } catch (Exception ex) {
                            throw new WayangException("Writing message to Kafka topic failed.", ex);
                        }
                    }
            );
        } catch (Exception e) {
            throw new WayangException("Writing to Kafka topic failed.", e);
        }

        logger.info("### 11 ... ");

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.java.kafkatopicsink.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                JavaExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.formattingDescriptor, configuration);
        return optEstimator;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException();
    }

}
