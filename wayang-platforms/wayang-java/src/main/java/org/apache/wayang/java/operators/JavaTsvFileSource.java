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

import org.apache.commons.io.IOUtils;
import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.core.util.fs.FileUtils;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link Operator} for the {@link JavaPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see JavaObjectFileSink
 */
public class JavaTsvFileSource<T> extends UnarySource<T> implements JavaExecutionOperator {

    private final String sourcePath;

    public JavaTsvFileSource(String sourcePath, DataSetType type) {
        super(type);
        this.sourcePath = sourcePath;
    }

    public JavaTsvFileSource(DataSetType<Tuple2> type) {
        this(null, type);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert outputs.length == this.getNumOutputs();

        final String path;
        if (this.sourcePath == null) {
            final FileChannel.Instance input = (FileChannel.Instance) inputs[0];
            path = input.getSinglePath();
        } else {
            assert inputs.length == 0;
            path = this.sourcePath;
        }
        final String actualInputPath = FileSystems.findActualSingleInputPath(path);
        Stream<T> stream = this.createStream(actualInputPath);
        ((StreamChannel.Instance) outputs[0]).accept(stream);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    private Stream<T> createStream(String path) {
        Function<String, T> parser = this::lineParse;

        return FileUtils.streamLines(path).map(parser);
    }

    private T lineParse(String line) {
        // TODO rewrite in less verbose way.
        Class typeClass = this.getType().getDataUnitType().getTypeClass();
        int tabPos = line.indexOf('\t');
        if (tabPos == -1) {
            if (typeClass == Integer.class) {
                return (T) Integer.valueOf(line);
            } else if (typeClass == Float.class) {
                return (T) Float.valueOf(line);
            } else if (typeClass == String.class) {
                return (T) String.valueOf(line);
            } else throw new WayangException(String.format("Cannot parse TSV file line %s", line));
        } else if (typeClass == Record.class) {
            // TODO: Fix Record parsing.
            return (T) new Record();
        } else if (typeClass == Tuple2.class) {
            // TODO: Fix Tuple2 parsing
            return (T) new Tuple2(
                    Integer.valueOf(line.substring(0, tabPos)),
                    Float.valueOf(line.substring(tabPos + 1)));
        } else
            throw new WayangException(String.format("Cannot parse TSV file line %s", line));

    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.java.tsvfilesource.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaTsvFileSource<>(this.sourcePath, this.getType());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_TSV_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    @Override public boolean isConversion() {
        return true;
    }
}
