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

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.basic.operators.ObjectFileSink;
import org.apache.wayang.basic.operators.TextFileSink;
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
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.logging.log4j.LogManager;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * {@link Operator} for the {@link JavaPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see JavaObjectFileSource
 */
public class JavaObjectFileSink<T> extends ObjectFileSink<T> implements JavaExecutionOperator {

    public JavaObjectFileSink(ObjectFileSink<T> that) {
        super(that);
    }

    public JavaObjectFileSink(DataSetType<T> type) {
        this(null, type);
    }

    public JavaObjectFileSink(String targetPath, DataSetType<T> type) {
        super(targetPath, type);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();

        // Prepare Hadoop's SequenceFile.Writer.
        final String path;
        FileChannel.Instance output;
        if(outputs.length == 1) {
            output = (FileChannel.Instance) outputs[0];
            path = output.addGivenOrTempPath(this.textFileUrl,
                javaExecutor.getCompiler().getConfiguration());
        }else{
            path = this.textFileUrl;
        }
        final SequenceFile.Writer.Option fileOption = SequenceFile.Writer.file(new Path(path));
        final SequenceFile.Writer.Option keyClassOption = SequenceFile.Writer.keyClass(NullWritable.class);
        final SequenceFile.Writer.Option valueClassOption = SequenceFile.Writer.valueClass(BytesWritable.class);
        try (SequenceFile.Writer writer = SequenceFile.createWriter(new Configuration(true), fileOption, keyClassOption, valueClassOption)) {

            // Chunk the stream of data quanta and write the chunks into the sequence file.
            StreamChunker streamChunker = new StreamChunker(10, (chunk, size) -> {
                if (chunk.length != size) {
                    System.out.println("heer");
                    System.out.println(chunk.length);
                    System.out.println(size);
                    chunk = Arrays.copyOfRange(chunk, 0, size);
                }
                try {
                    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    final ObjectOutputStream oos = new ObjectOutputStream(bos);
                    oos.writeObject(chunk);
                    BytesWritable bytesWritable = new BytesWritable(bos.toByteArray());
                    writer.append(NullWritable.get(), bytesWritable);
                } catch (IOException e) {
                    throw new UncheckedIOException("Writing or serialization failed.", e);
                }
            });
            ((JavaChannelInstance) inputs[0]).provideStream().forEach(streamChunker::push);
            streamChunker.fire();
            LogManager.getLogger(this.getClass()).info("Writing dataset to {}.", path);
        } catch (IOException | UncheckedIOException e) {
            throw new WayangException("Could not write stream to sequence file.", e);
        }

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.java.objectfilesink.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaObjectFileSink<>(this.textFileUrl, this.getType());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Collections.singletonList(FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR);
    }

    @Override public boolean isConversion() {
        return true;
    }

    /**
     * Utility to chunk a {@link Stream} into portions of fixed size.
     */
    public static class StreamChunker {

        private final BiConsumer<Object[], Integer> action;

        private final Object[] chunk;

        private int nextIndex;

        private long numPushedObjects = 0L;

        public StreamChunker(int chunkSize, BiConsumer<Object[], Integer> action) {
            this.action = action;
            this.chunk = new Object[chunkSize];
            this.nextIndex = 0;
        }

        /**
         * Add a new element to the current chunk. Fire, if the chunk is complete.
         */
        public void push(Object obj) {
            this.numPushedObjects++;
            this.chunk[this.nextIndex] = obj;
            if (++this.nextIndex >= this.chunk.length) {
                this.fire();
            }
        }

        /**
         * Apply the specified {@link #action} to the current chunk and prepare for a new chunk.
         */
        public void fire() {
            if (this.nextIndex > 0) {
                this.action.accept(this.chunk, this.nextIndex);
                this.nextIndex = 0;
            }
        }

    }
}
