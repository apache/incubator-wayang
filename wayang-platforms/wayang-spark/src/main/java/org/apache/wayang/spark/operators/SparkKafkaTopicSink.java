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

package org.apache.wayang.spark.operators;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.wayang.basic.operators.KafkaTopicSink;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;

import java.io.Serializable;
import java.util.*;

/**
 * Implementation of the {@link KafkaTopicSink} operator for the Spark platform.
 *
 * @see SparkKafkaTopicSource
 */
public class SparkKafkaTopicSink<T> extends KafkaTopicSink<T> implements SparkExecutionOperator, Serializable {

    public SparkKafkaTopicSink() {
        super();
    }

    public SparkKafkaTopicSink(String textFileUrl, TransformationDescriptor<T, String> formattingDescriptor) {
        super(textFileUrl, formattingDescriptor);
    }

    public SparkKafkaTopicSink(KafkaTopicSink<T> that) {
        super(that);
    }


    public void writeToKafka(JavaRDD<T> inputRdd, final String topicName) {

        inputRdd.foreachPartition(new VoidFunction<Iterator<T>>() {

            @Override
            public void call(Iterator<T> partition) throws Exception {
                // Kafka producer properties
                KafkaProducer<String, String> producer = getProducer();

                // Send each record of the partition to Kafka
                while (partition.hasNext()) {
                    String kvp = (String) partition.next(); // Apply formatting function if necessary
                    producer.send(new ProducerRecord<>(topicName, "-", kvp));
                }

                // Close the producer
                producer.close();
            }
        });
    }


    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == 1;

        assert outputs.length == 0;

        this.initProducer(this);

        JavaRDD<T> inputRdd = ((RddChannel.Instance) inputs[0]).provideRdd();
        final Function<T, String> formattingFunction =
                sparkExecutor.getCompiler().compile(this.formattingDescriptor, this, operatorContext, inputs);

        writeToKafka( inputRdd, this.topicName );

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no outputs.");
    }

    @Override
    public boolean containsAction() {
        return true;
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.textfilesink.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                SparkExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.formattingDescriptor, configuration);
        return optEstimator;
    }

}
