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

package org.apache.wayang.flink.operators;

import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.core.fs.FileSystem;
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
import org.apache.wayang.flink.channels.DataSetChannel;
import org.apache.wayang.flink.compiler.WayangFileOutputFormat;
import org.apache.wayang.flink.execution.FlinkExecutor;
import org.apache.wayang.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * {@link Operator} for the {@link FlinkPlatform} that creates a sequence file.
 *
 * @see FlinkObjectFileSink
 */
public class FlinkObjectFileSink<Type> extends ObjectFileSink<Type> implements FlinkExecutionOperator {

    public FlinkObjectFileSink(ObjectFileSink<Type> that) {
        super(that);
    }

    public FlinkObjectFileSink(DataSetType<Type> type) {
        this(null, type);
    }

    public FlinkObjectFileSink(String targetPath, DataSetType<Type> type) {
        super(targetPath, type);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext
    ) throws Exception {

        assert inputs.length == this.getNumInputs();
        assert outputs.length <= 1;
        final FileChannel.Instance output;
        final String targetPath;
        if(outputs.length == 1) {
            output = (FileChannel.Instance) outputs[0];
            targetPath = output.addGivenOrTempPath(this.textFileUrl, flinkExecutor.getConfiguration());
        }else{
            targetPath = this.textFileUrl;
        }

        //TODO: remove the set parallelism 1
        DataSetChannel.Instance input = (DataSetChannel.Instance) inputs[0];
        final DataSink<Type> tDataSink = input.<Type>provideDataSet()
                .write(new WayangFileOutputFormat<Type>(targetPath), targetPath, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(flinkExecutor.fee.getParallelism());


        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkObjectFileSink<>(this.textFileUrl, this.getType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.flink.objectfilesink.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
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
