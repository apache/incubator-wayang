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

import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.basic.operators.ObjectFileSink;
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
import org.apache.logging.log4j.LogManager;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a sequence file.
 *
 * @see SparkObjectFileSource
 */
public class SparkObjectFileSink<T> extends ObjectFileSink<T> implements SparkExecutionOperator {

    public SparkObjectFileSink(ObjectFileSink<T> that) {
        super(that);
    }

    public SparkObjectFileSink(DataSetType<T> type) {
        this(null, type);
    }

    public SparkObjectFileSink(String targetPath, DataSetType<T> type) {
        super(targetPath, type);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length <= 1;

        final String targetPath;
        if(outputs.length > 0) {
            final FileChannel.Instance output = (FileChannel.Instance) outputs[0];
            targetPath = output.addGivenOrTempPath(this.textFileUrl, sparkExecutor.getConfiguration());
        }else{
            targetPath = this.textFileUrl;
        }

        RddChannel.Instance input = (RddChannel.Instance) inputs[0];

        input.provideRdd()
                .coalesce(1) // TODO: Remove. This only hotfixes the issue that JavaObjectFileSource reads only a single file.
                .saveAsObjectFile(targetPath);
        LogManager.getLogger(this.getClass()).info("Writing dataset to {}.", targetPath);

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkObjectFileSink<>(this.textFileUrl, this.getType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.objectfilesink.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return true;
    }

    @Override public boolean isConversion() {
        return true;
    }
}
