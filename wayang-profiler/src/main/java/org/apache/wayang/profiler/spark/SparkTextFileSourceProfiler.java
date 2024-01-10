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

package org.apache.wayang.profiler.spark;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.spark.operators.SparkTextFileSource;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.function.Supplier;

/**
 * {@link SparkOperatorProfiler} for the {@link SparkTextFileSource}.
 */
public class SparkTextFileSourceProfiler extends SparkSourceProfiler {

    private final String fileUrl;

    public SparkTextFileSourceProfiler(Configuration configuration,
                                       Supplier<?> dataQuantumGenerator) {
        this(configuration.getStringProperty("wayang.profiler.datagen.url"), configuration, dataQuantumGenerator);
    }

    private SparkTextFileSourceProfiler(String fileUrl,
                                        Configuration configuration,
                                        Supplier<?> dataQuantumGenerator) {
        super(() -> new SparkTextFileSource(fileUrl), configuration, dataQuantumGenerator);
        this.fileUrl = fileUrl;
    }

    @Override
    protected void prepareInput(int inputIndex, long inputCardinality) {
        assert inputIndex == 0;

        // Obtain access to the file system.
        final FileSystem fileSystem = FileSystems.getFileSystem(this.fileUrl).orElseThrow(
                () -> new WayangException(String.format("File system of %s not supported.", this.fileUrl))
        );

        // Try to delete any existing file.
        try {
            if (!fileSystem.delete(this.fileUrl, true)) {
                this.logger.warn("Could not delete {}.", this.fileUrl);
            }
        } catch (IOException e) {
            this.logger.error(String.format("Deleting %s failed.", this.fileUrl), e);
        }

        // Generate and write the test data.
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(
                        fileSystem.create(this.fileUrl),
                        "UTF-8"
                )
        )) {
            final Supplier<?> supplier = this.dataQuantumGenerators.get(0);
            for (long i = 0; i < inputCardinality; i++) {
                writer.write(supplier.get().toString());
                writer.write('\n');
            }
        } catch (Exception e) {
            throw new WayangException(String.format("Could not write test data to %s.", this.fileUrl), e);
        }
    }

    @Override
    public void cleanUp() {
        super.cleanUp();

        FileSystems.getFileSystem(this.fileUrl).ifPresent(fs -> {
                    try {
                        fs.delete(this.fileUrl, true);
                    } catch (IOException e) {
                        this.logger.error(String.format("Could not delete profiling file %s.", this.fileUrl), e);
                    }
                }
        );
    }
}
