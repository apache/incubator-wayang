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

package org.apache.wayang.basic.channels;

import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.AbstractChannelInstance;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.Executor;

public class SingleObjectChannel extends Channel {

    public static final SingleObjectChannel.Descriptor DEFAULT_DESCRIPTOR = new SingleObjectChannel.Descriptor();

    public SingleObjectChannel(SingleObjectChannel.Descriptor descriptor) {
        this(descriptor, null);
    }

    public SingleObjectChannel(ChannelDescriptor descriptor, OutputSlot<?> producerSlot) {
        super(descriptor, producerSlot);
    }

    public SingleObjectChannel(SingleObjectChannel parent) {
        super(parent);
    }

    @Override
    public Channel copy() {
        return new SingleObjectChannel(this);
    }

    @Override
    public SingleObjectChannel.Descriptor getDescriptor() {
        return (SingleObjectChannel.Descriptor) super.getDescriptor();
    }

    @Override
    public ChannelInstance createInstance(Executor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
        return new Instance(producerOperatorContext, producerOutputIndex);
    }

    public static class Descriptor extends ChannelDescriptor {

        public Descriptor() {
            // TODO
            super(SingleObjectChannel.class, false, false);
        }
    }

    public class Instance extends AbstractChannelInstance {
        private Object obj;

        /**
         * Creates a new instance.
         */
        public Instance(OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
            super(null, producerOperatorContext, producerOutputIndex);
        }

        public void accept(Object obj) {
            this.obj = obj;
        }

        public Object provideObj() {
            return this.obj;
        }

        @Override
        public SingleObjectChannel getChannel() {
            return SingleObjectChannel.this;
        }

        @Override
        public void doDispose() throws WayangException {
            // TODO
        }
    }
}
