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

package org.apache.wayang.profiler.java;

import org.apache.wayang.java.operators.JavaTextFileSource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.function.Supplier;

/**
 * {@link OperatorProfiler} for sources.
 */
public class JavaTextFileSourceProfiler extends SourceProfiler {

    private File tempFile;

    public JavaTextFileSourceProfiler(Supplier<String> dataQuantumGenerator, String fileUrl) {
        super(() -> new JavaTextFileSource(fileUrl), dataQuantumGenerator);
    }

    @Override
    void setUpSourceData(long cardinality) throws Exception {
        if (this.tempFile != null) {
            if (!this.tempFile.delete()) {
                this.logger.warn("Could not delete {}.", this.tempFile);
            }
        }
        this.tempFile = File.createTempFile("wayang-java", "txt");
        this.tempFile.deleteOnExit();

        // Create input data.
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(this.tempFile))) {
            final Supplier<?> supplier = this.dataQuantumGenerators.get(0);
            for (int i = 0; i < cardinality; i++) {
                writer.write(supplier.get().toString());
                writer.write('\n');
            }
        }
    }

    @Override
    protected long provideDiskBytes() {
        return this.tempFile.length();
    }


}
