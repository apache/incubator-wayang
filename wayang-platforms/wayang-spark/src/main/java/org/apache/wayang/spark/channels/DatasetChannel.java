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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.AbstractChannelInstance;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.util.Actions;
import org.apache.wayang.spark.execution.SparkExecutor;

/**
 * {@link Channel} implementation that transports Spark {@link Dataset}s (i.e., DataFrames).
 */
public class DatasetChannel extends Channel {

    public static final ChannelDescriptor UNCACHED_DESCRIPTOR = new ChannelDescriptor(
            DatasetChannel.class, false, false
    );

    public static final ChannelDescriptor CACHED_DESCRIPTOR = new ChannelDescriptor(
            DatasetChannel.class, true, true
    );

    public DatasetChannel(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
        assert descriptor == UNCACHED_DESCRIPTOR || descriptor == CACHED_DESCRIPTOR;
    }

    private DatasetChannel(DatasetChannel parent) {
        super(parent);
    }

    @Override
    public DatasetChannel copy() {
        return new DatasetChannel(this);
    }

    @Override
    public Instance createInstance(Executor executor,
                                   OptimizationContext.OperatorContext producerOperatorContext,
                                   int producerOutputIndex) {
        return new Instance((SparkExecutor) executor, producerOperatorContext, producerOutputIndex);
    }

    /**
     * {@link ChannelInstance} for {@link DatasetChannel}s.
     */
    public class Instance extends AbstractChannelInstance {

        private Dataset<Row> dataset;

        public Instance(SparkExecutor executor,
                        OptimizationContext.OperatorContext producerOperatorContext,
                        int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }

        /**
         * Store a {@link Dataset} in this channel and optionally measure its cardinality.
         *
         * @param dataset       the {@link Dataset} to store
         * @param sparkExecutor the {@link SparkExecutor} handling this channel
         */
        public void accept(Dataset<Row> dataset, SparkExecutor sparkExecutor) {
            this.dataset = dataset;
            if (this.isMarkedForInstrumentation()) {
                this.measureCardinality(dataset);
            }
        }

        /**
         * Provide the stored {@link Dataset}.
         *
         * @return the stored {@link Dataset}
         */
        public Dataset<Row> provideDataset() {
            return this.dataset;
        }

        @Override
        protected void doDispose() {
            if (this.isDatasetCached() && this.dataset != null) {
                Actions.doSafe(() -> this.dataset.unpersist());
                this.dataset = null;
            }
        }

        private void measureCardinality(Dataset<Row> dataset) {
            this.setMeasuredCardinality(dataset.count());
        }

        private boolean isDatasetCached() {
            return this.getChannel().isReusable();
        }

        @Override
        public DatasetChannel getChannel() {
            return DatasetChannel.this;
        }
    }
}
