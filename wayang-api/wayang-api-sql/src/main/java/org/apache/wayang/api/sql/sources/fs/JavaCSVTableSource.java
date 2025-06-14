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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JavaCSVTableSource<T> extends UnarySource<T> implements JavaExecutionOperator {

    private final String sourcePath;

    private final List<RelDataType> fieldTypes;
    private final char separator; // Default separator

    // TODO: incorporate fields later for projectable table scans
    // private final ImmutableIntList fields;

    /**
     * Table source with default seperator ';' <p>
     * See {@link #JavaCSVTableSource(String, DataSetType, List, char)} for custom seperator
     * @param sourcePath
     * @param type
     * @param fieldTypes
     */
    public JavaCSVTableSource(final String sourcePath, final DataSetType<T> type, final List<RelDataType> fieldTypes) {
        super(type);
        this.sourcePath = sourcePath;
        this.fieldTypes = fieldTypes;
        this.separator = ';'; // Default seperator
    }

    /**
     * Constructor with custom separator
     * 
     * @param sourcePath
     * @param type
     * @param fieldTypes
     * @param separator
     */
    public JavaCSVTableSource(final String sourcePath, final DataSetType<T> type, final List<RelDataType> fieldTypes,
            final char separator) {
        super(type);
        this.sourcePath = sourcePath;
        this.fieldTypes = fieldTypes;
        this.separator = separator;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final JavaExecutor javaExecutor,
            final OptimizationContext.OperatorContext operatorContext) {
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
        final Stream<Record> stream = this.createStream(actualInputPath);
        ((StreamChannel.Instance) outputs[0]).accept(stream);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    private Stream<Record> createStream(final String actualInputPath) {
        return streamLines(actualInputPath).map(this::parseLine);
    }

    private Record parseLine(final String s) {
        assert this.getType().getDataUnitType().getTypeClass() == Record.class;

        try {
            final String[] tokens = CsvRowConverter.parseLine(s, this.separator);
            if (tokens.length != fieldTypes.size())
                throw new IllegalStateException(
                        String.format("Error while parsing CSV file %s at line %s, using separator %s", sourcePath, s, separator));
            // now tokens.length == fieldtypes.size

            final Object[] objects = new Object[tokens.length];

            for (int i = 0; i < tokens.length; i++) {
                objects[i] = CsvRowConverter.convert(fieldTypes.get(i), tokens[i]);
            }

            return new Record(objects);
        } catch (final IOException e) {
            e.printStackTrace();
            throw new IllegalStateException(String.format("Error while parsing CSV file %s at line %s", sourcePath, s));
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        return Collections.singletonList(FileChannel.HDFS_TSV_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    /**
     * Copied from {@link FileUtils} as a quick work around to read CSV file after
     * skipping
     * header row.
     *
     * Creates a {@link Stream} of a lines of the file.
     *
     * @param path of the file
     * @return the {@link Stream}
     */
    private static Stream<String> streamLines(final String path) {
        final FileSystem fileSystem = FileSystems.getFileSystem(path).orElseThrow(
                () -> new IllegalStateException(String.format("No file system found for %s", path)));
        try {
            final Iterator<String> lineIterator = createLineIterator(fileSystem, path);
            lineIterator.next(); // skip header row
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(lineIterator, 0), false);
        } catch (final IOException e) {
            throw new WayangException(String.format("%s failed to read %s.", FileUtils.class, path), e);
        }

    }

    /**
     * Creates an {@link Iterator} over the lines of a given {@code path} (that
     * resides in the given {@code fileSystem}).
     */
    private static Iterator<String> createLineIterator(final FileSystem fileSystem, final String path)
            throws IOException {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(path), "UTF-8"));
        return new Iterator<String>() {

            String next;

            {
                this.advance();
            }

            private void advance() {
                try {
                    this.next = reader.readLine();
                } catch (final IOException e) {
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
