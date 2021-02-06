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

package org.apache.wayang.spark.channels;

import org.apache.spark.broadcast.Broadcast;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.AbstractChannelInstance;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.spark.execution.SparkExecutor;

/**
 * {@link Channel} that represents a broadcasted value.
 */
public class BroadcastChannel extends Channel {

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(
            BroadcastChannel.class, true, true);

    public BroadcastChannel(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
    }

    private BroadcastChannel(BroadcastChannel parent) {
        super(parent);
    }


    @Override
    public BroadcastChannel copy() {
        return new BroadcastChannel(this);
    }

    @Override
    public Instance createInstance(Executor executor,
                                   OptimizationContext.OperatorContext producerOperatorContext,
                                   int producerOutputIndex) {
        return new Instance((SparkExecutor) executor, producerOperatorContext, producerOutputIndex);
    }

    /**
     * {@link ChannelInstance} implementation for {@link BroadcastChannel}s.
     */
    public class Instance extends AbstractChannelInstance {

        private Broadcast<?> broadcast;

        public Instance(SparkExecutor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }

        public void accept(Broadcast broadcast) {
            assert this.broadcast == null : String.format("Broadcast for %s already initialized.", this.getChannel());
            this.broadcast = broadcast;
        }

        @SuppressWarnings("unchecked")
        public Broadcast<?> provideBroadcast() {
            assert this.broadcast != null : String.format("Broadcast for %s not initialized.", this.getChannel());
            return this.broadcast;
        }

        @Override
        protected void doDispose() {
         // TODO: We must not dispose broadcasts for now because they can break lazy computation.
//            this.doSafe(() -> this.broadcast.destroy(false));
        }

        @Override
        public BroadcastChannel getChannel() {
            return BroadcastChannel.this;
        }

    }

}
