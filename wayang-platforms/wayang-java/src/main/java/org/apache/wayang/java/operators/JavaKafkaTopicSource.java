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


import org.apache.wayang.basic.operators.KafkaTopicSource;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;
import java.time.Duration;
import java.util.Collections;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class JavaKafkaTopicSource extends KafkaTopicSource implements JavaExecutionOperator {

    public JavaKafkaTopicSource(String topicName) {
        super(topicName);
    }

    public JavaKafkaTopicSource(String topicName, String encoding) {
        super(topicName, encoding);
    }

    public JavaKafkaTopicSource(KafkaTopicSource that) {
        super(that);
    }


    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("wayang.java.kafkatopicsource.load.prepare", "wayang.java.kafkatopicsource.load.main");
    }


    @Override
    public JavaKafkaTopicSource copy() {
        return new JavaKafkaTopicSource(this.getTopicName(), this.getEncoding());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        try {

            // Poll messages from the topic
            System.out.println("### 7 ... ");
            this.initConsumer( (KafkaTopicSource) this );

            ConsumerRecords<String, String> records = getConsumer().poll(Duration.ofMillis(15000));

            System.out.println("### 8 ... ");

            // Convert the records into a Stream<String>
            Stream<String> messageStream = StreamSupport.stream(records.spliterator(), false)
                    .map(ConsumerRecord::value); // Extract the message value

            // Use the message stream as needed
            // For example, you can print each message
            // => messageStream.forEach(System.out::println);

            ((StreamChannel.Instance) outputs[0]).accept(messageStream);

        }
        catch (Exception ioException) {
            ioException.printStackTrace();
            throw new WayangException(String.format("ERROR WHILE READING FROM KAFKA TOPIC in JavaKafkaTopicSource [%s].", getTopicName() ));
        }


        ExecutionLineageNode prepareLineageNode = new ExecutionLineageNode(operatorContext);
        prepareLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.kafkatopicsource.load.prepare", javaExecutor.getConfiguration()
        ));
        ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);
        mainLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.kafkatopicsource.load.main", javaExecutor.getConfiguration()
        ));

        outputs[0].getLineage().addPredecessor(mainLineageNode);

        return prepareLineageNode.collectAndMark();
    }

}


// source build/env.sh; mvn exec:java -pl wayang-platforms/wayang-java -Dexec.mainClass="org.apache.wayang.java.operators.JavaKafkaTopicSource"
