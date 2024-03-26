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

package org.apache.wayang.tensorflow.channels;

import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.AbstractChannelInstance;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.tensorflow.execution.TensorflowExecutor;
import org.tensorflow.ndarray.NdArray;

import java.util.OptionalLong;

public class TensorChannel extends Channel {

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(
            TensorChannel.class, false, false
    );

    public TensorChannel(OutputSlot<?> producerSlot) {
        super(DESCRIPTOR, producerSlot);
    }

    public TensorChannel(ChannelDescriptor descriptor, OutputSlot<?> producerSlot) {
        super(descriptor, producerSlot);
        assert descriptor == DESCRIPTOR;
    }

    private TensorChannel(TensorChannel parent) {
        super(parent);
    }

    @Override
    public Channel copy() {
        return new TensorChannel(this);
    }

    @Override
    public ChannelInstance createInstance(
            Executor executor,
            OptimizationContext.OperatorContext producerOperatorContext,
            int producerOutputIndex) {
        return new Instance((TensorflowExecutor) executor, producerOperatorContext, producerOutputIndex);
    }

    /**
     * {@link ChannelInstance} implementation for {@link TensorChannel}s.
     */
    public class Instance extends AbstractChannelInstance {

        // The first dimension represents the number of entries
        private NdArray<?> tensor;
        private long cardinality = 0;

        public Instance(
                TensorflowExecutor executor,
                OptimizationContext.OperatorContext producerOperatorContext,
                int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }

        public void accept(NdArray<?> tensor) {
            assert this.tensor == null;
            this.tensor = tensor;
            if (this.isMarkedForInstrumentation()) {
                cardinality = tensor.shape().size(0);
            }
        }

        public <T extends NdArray<?>> T provideTensor() {
            return (T) this.tensor;
        }

        @Override
        public Channel getChannel() {
            return TensorChannel.this;
        }

        @Override
        public OptionalLong getMeasuredCardinality() {
            return this.cardinality == 0 ? super.getMeasuredCardinality() : OptionalLong.of(this.cardinality);
        }

        @Override
        protected void doDispose() throws Throwable {
            this.tensor = null;
        }
    }
}
