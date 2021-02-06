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

package org.apache.wayang.java.channels;

import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.AbstractChannelInstance;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.java.operators.JavaExecutionOperator;

import java.util.Collection;
import java.util.OptionalLong;
import java.util.stream.Stream;

/**
 * {@link Channel} between two {@link JavaExecutionOperator}s using a {@link Stream}.
 */
public class StreamChannel extends Channel {

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(StreamChannel.class, false, false);

    public StreamChannel(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
        assert descriptor == DESCRIPTOR;
    }

    private StreamChannel(StreamChannel parent) {
        super(parent);
    }

    @Override
    public StreamChannel copy() {
        return new StreamChannel(this);
    }

//    public Channel exchangeWith(ChannelDescriptor descriptor) {
//        // Todo: Hacky.
//        final ExecutionTask producer = this.getProducer();
//        final OutputSlot<?> outputSlot = producer.getOutputSlotFor(this);
//
//        final ChannelManager channelManager = producer.getPlatform().getChannelManager();
//        final ChannelInitializer channelInitializer = channelManager.getChannelInitializer(descriptor);
//        final Tuple<Channel, Channel> newChannelSetup = channelInitializer.setUpOutput(descriptor, outputSlot, null);
//
//        int outputIndex = producer.removeOutputChannel(this);
//        producer.setOutputChannel(outputIndex, newChannelSetup.field0);
//
//        for (ExecutionTask consumer : new ArrayList<>(this.getConsumers())) {
//            consumer.exchangeInputChannel(this, newChannelSetup.field1);
//        }
//        this.addSibling(newChannelSetup.field0);
//        this.removeSiblings();
//
//        if (this.isMarkedForInstrumentation()) {
//            newChannelSetup.field0.markForInstrumentation();
//        }
//
//        return newChannelSetup.field1;
//    }

    @Override
    public Instance createInstance(Executor executor,
                                   OptimizationContext.OperatorContext producerOperatorContext,
                                   int producerOutputIndex) {
        return new Instance(executor, producerOperatorContext, producerOutputIndex);
    }

//    public static class Initializer implements JavaChannelInitializer {
//
//        @Override
//        public Tuple<Channel, Channel> setUpOutput(ChannelDescriptor descriptor, OutputSlot<?> outputSlot, OptimizationContext optimizationContext) {
//            StreamChannel channel = new StreamChannel(descriptor, outputSlot);
//            return new Tuple<>(channel, channel);
//        }
//
//        @Override
//        public Channel setUpOutput(ChannelDescriptor descriptor, Channel source, OptimizationContext optimizationContext) {
//            assert descriptor == StreamChannel.DESCRIPTOR;
//            final JavaChannelInitializer channelInitializer = this.getChannelManager().getChannelInitializer(source.getDescriptor());
//            return channelInitializer.provideStreamChannel(source, optimizationContext);
//        }
//
//        @Override
//        public StreamChannel provideStreamChannel(Channel channel, OptimizationContext optimizationContext) {
//            return (StreamChannel) channel;
//        }
//    }

    /**
     * {@link JavaChannelInstance} implementation for {@link StreamChannel}s.
     */
    public class Instance extends AbstractChannelInstance implements JavaChannelInstance {

        private Stream<?> stream;

        // In principle, we could use Stream#onClose() to make sure that we really counted the cardinality (so as to
        // detect, when the cardinality is 0 because the #stream has not been fully executed for whatever reason).
        // However, this would require to call Stream#close() on all methods.
        private long cardinality = 0;

        public Instance(Executor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }


        public <T> void accept(Stream<T> stream) {
            assert this.stream == null;
            this.stream = stream;
            if (this.isMarkedForInstrumentation()) {
                this.stream = this.stream.filter(dataQuantum -> {
                    this.cardinality += 1;
                    return true;
                });
            }
        }

        public void accept(Collection<?> collection) {
            assert this.stream == null;
            this.stream = collection.stream();
            this.setMeasuredCardinality(collection.size());
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Stream<T> provideStream() {
            return (Stream<T>) this.stream;
        }

        @Override
        public Channel getChannel() {
            return StreamChannel.this;
        }

        @Override
        public OptionalLong getMeasuredCardinality() {
            return this.cardinality == 0 ? super.getMeasuredCardinality() : OptionalLong.of(this.cardinality);
        }

        @Override
        protected void doDispose() throws WayangException {
            this.stream = null;
        }
    }

}
