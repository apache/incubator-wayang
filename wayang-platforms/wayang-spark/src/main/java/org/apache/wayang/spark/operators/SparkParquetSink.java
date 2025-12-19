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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.ParquetSink;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spark.channels.DatasetChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.util.DatasetConverters;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Writes records to Parquet using Spark.
 */
public class SparkParquetSink extends ParquetSink implements SparkExecutionOperator {

    private final SaveMode saveMode;

    public SparkParquetSink(ParquetSink that) {
        super(that.getOutputUrl(), that.isOverwrite(), that.prefersDataset(), that.getType());
        this.saveMode = that.isOverwrite() ? SaveMode.Overwrite : SaveMode.ErrorIfExists;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(ChannelInstance[] inputs,
                                                                                        ChannelInstance[] outputs,
                                                                                        SparkExecutor sparkExecutor,
                                                                                        OptimizationContext.OperatorContext operatorContext) {
        final Dataset<Row> dataset = this.obtainDataset(inputs[0], sparkExecutor);
        dataset.write().mode(this.saveMode).parquet(this.getOutputUrl());
        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    private Dataset<Row> obtainDataset(ChannelInstance input, SparkExecutor sparkExecutor) {
        if (input instanceof DatasetChannel.Instance) {
            return ((DatasetChannel.Instance) input).provideDataset();
        }
        JavaRDD<Record> rdd = ((RddChannel.Instance) input).provideRdd();
        return DatasetConverters.recordsToDataset(rdd, this.getType(), sparkExecutor.ss);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        if (this.prefersDataset()) {
            return Arrays.asList(DatasetChannel.UNCACHED_DESCRIPTOR, DatasetChannel.CACHED_DESCRIPTOR);
        }
        return Arrays.asList(DatasetChannel.UNCACHED_DESCRIPTOR, DatasetChannel.CACHED_DESCRIPTOR,
                RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no outputs.");
    }

    @Override
    public boolean containsAction() {
        return true;
    }
}
