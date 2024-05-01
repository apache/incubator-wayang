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
import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.UnarySink;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.platform.SparkPlatform;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a TSV file.
 * Only applicable to tuples with standard datatypes.
 *
 * @see SparkObjectFileSource
 */
public class SparkTsvFileSink<T extends Tuple2<?, ?>> extends UnarySink<T> implements SparkExecutionOperator {

    private final String targetPath;

    public SparkTsvFileSink(DataSetType<T> type) {
        this(null, type);
    }

    public SparkTsvFileSink(String targetPath, DataSetType<T> type) {
        super(type);
        assert type.equals(DataSetType.createDefault(Tuple2.class)) :
                String.format("Illegal type for %s: %s", this, type);
        this.targetPath = targetPath;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();

        final FileChannel.Instance output = (FileChannel.Instance) outputs[0];
        final String targetPath = output.addGivenOrTempPath(this.targetPath, sparkExecutor.getConfiguration());

        final RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        final JavaRDD<Object> rdd = input.provideRdd();
        final JavaRDD<String> serializedRdd = rdd
                .map(dataQuantum -> {
                    // TODO: Once there are more tuple types, make this generic.
                    @SuppressWarnings("unchecked")
                    Tuple2<Object, Object> tuple2 = (Tuple2<Object, Object>) dataQuantum;
                    return String.valueOf(tuple2.field0) + '\t' + String.valueOf(tuple2.field1);
                });
        this.name(serializedRdd);
        serializedRdd
                .coalesce(1) // TODO: Allow more than one TSV file?
                .saveAsTextFile(targetPath);


        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkTsvFileSink<>(this.targetPath, this.getType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.tsvfilesink.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_TSV_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return true;
    }

    @Override public boolean isConversion() {
        return true;
    }
}
