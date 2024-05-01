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

import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.UnarySink;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * {@link Operator} for the {@link JavaPlatform} that creates a TSV file.
 * Only applicable to tuples with standard datatypes.
 *
 * @see JavaObjectFileSource
 */
public class JavaTsvFileSink<T extends Tuple2<?, ?>> extends UnarySink<T> implements JavaExecutionOperator {

    private final String targetPath;

    public JavaTsvFileSink(DataSetType<T> type) {
        this(null, type);
    }

    public JavaTsvFileSink(String targetPath, DataSetType<T> type) {
        super(type);
        assert type.equals(DataSetType.createDefault(Tuple2.class)) :
                String.format("Illegal type for %s: %s", this, type);
        this.targetPath = targetPath;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();

        // Prepare Hadoop's SequenceFile.Writer.
        FileChannel.Instance output = (FileChannel.Instance) outputs[0];
        final String path = output.addGivenOrTempPath(this.targetPath, javaExecutor.getCompiler().getConfiguration());
        final FileSystem fileSystem = FileSystems.getFileSystem(path).orElseThrow(
                () -> new IllegalStateException(String.format("No file system found for \"%s\".", this.targetPath))
        );

        try (final BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(
                        fileSystem.create(path), "UTF-8"
                )
        )) {
            try {
                ((JavaChannelInstance) inputs[0]).provideStream().forEach(
                        dataQuantum -> {
                            try {
                                // TODO: Once there are more tuple types, make this generic.
                                @SuppressWarnings("unchecked")
                                Tuple2<Object, Object> tuple2 = (Tuple2<Object, Object>) dataQuantum;
                                writer.append(String.valueOf(tuple2.field0))
                                        .append('\t')
                                        .append(String.valueOf(tuple2.field1))
                                        .append('\n');
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }
                );
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        } catch (IOException e) {
            throw new WayangException(String.format("%s failed on writing to %s.", this, this.targetPath), e);
        }

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.java.tsvfilesink.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaTsvFileSink<>(this.targetPath, this.getType());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Collections.singletonList(FileChannel.HDFS_TSV_DESCRIPTOR);
    }

    @Override public boolean isConversion() {
        return true;
    }
}
