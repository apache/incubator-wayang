/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.sql.sources.fs;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.io.IOUtils;
import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
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
import org.apache.wayang.java.operators.JavaExecutionOperator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JavaCSVTableSource<T> extends UnarySource<T> implements JavaExecutionOperator {


    private final String sourcePath;

    private final List<RelDataType> fieldTypes;
    private char separator = ';';   // Default separator

    // TODO: incorporate fields later for projectable table scans
    // private final ImmutableIntList fields;

    public JavaCSVTableSource(String sourcePath, DataSetType type, List<RelDataType> fieldTypes) {
        super(type);
        this.sourcePath = sourcePath;
        this.fieldTypes = fieldTypes;
    }

    /**
     * Constructor with separator
     * @param sourcePath
     * @param type
     * @param fieldTypes
     * @param separator
     */
    public JavaCSVTableSource(String sourcePath, DataSetType type, List<RelDataType> fieldTypes, char separator) {
        super(type);
        this.sourcePath = sourcePath;
        this.fieldTypes = fieldTypes;
        this.separator = separator;
    }

    /*public JavaCSVTableSource(DataSetType<T> type) {
        this(null, type);
    }*/

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

    private Stream<T> createStream(String actualInputPath) {
        Function<String, T> parser = this::parseLine;
        return streamLines(actualInputPath).map(parser);
    }

    private T parseLine(String s) {
        Class typeClass = this.getType().getDataUnitType().getTypeClass();
        assert typeClass == Record.class;
        try {
            String[] tokens = CsvRowConverter.parseLine(s, this.separator);

            if (tokens.length != fieldTypes.size()) {
                throw new IllegalStateException(String.format("Error while parsing CSV file %s at line %s", sourcePath, s));
            }
            final Object[] objects = new Object[tokens.length];
            for (int i = 0; i < tokens.length; i++) {
                objects[i] = CsvRowConverter.convert(fieldTypes.get(i), tokens[i]);
            }
            return (T) new Record(objects);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        throw new IllegalStateException(String.format("Error while parsing CSV file %s at line %s", sourcePath, s));
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

    /**
     * Copied from {@link FileUtils} as a quick work around to read CSV file after skipping
     * header row.
     *
     * Creates a {@link Stream} of a lines of the file.
     *
     * @param path of the file
     * @return the {@link Stream}
     */
    private static Stream<String> streamLines(String path) {
        final FileSystem fileSystem = FileSystems.getFileSystem(path).orElseThrow(
                () -> new IllegalStateException(String.format("No file system found for %s", path))
        );
        try {
            Iterator<String> lineIterator = createLineIterator(fileSystem, path);
            lineIterator.next(); // skip header row
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(lineIterator, 0), false);
        } catch (IOException e) {
            throw new WayangException(String.format("%s failed to read %s.", FileUtils.class, path), e);
        }

    }

    /**
     * Creates an {@link Iterator} over the lines of a given {@code path} (that resides in the given {@code fileSystem}).
     */
    private static Iterator<String> createLineIterator(FileSystem fileSystem, String path) throws IOException {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(path), "UTF-8"));
        return new Iterator<String>() {

            String next;

            {
                this.advance();
            }

            private void advance() {
                try {
                    this.next = reader.readLine();
                } catch (IOException e) {
                    this.next = null;
                    throw new UncheckedIOException(e);
                } finally {
                    if (this.next == null) {
                        IOUtils.closeQuietly(reader);
                    }
                }
            }

            @Override
            public boolean hasNext() {
                return this.next != null;
            }

            @Override
            public String next() {
                assert this.hasNext();
                final String returnValue = this.next;
                this.advance();
                return returnValue;
            }
        };

    }





}
