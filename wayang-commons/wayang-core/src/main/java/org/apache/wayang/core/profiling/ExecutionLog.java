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

package org.apache.wayang.core.profiling;

import org.apache.commons.io.IOUtils;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.platform.PartialExecution;
import org.apache.wayang.core.util.JsonSerializables;
import org.apache.wayang.core.util.JsonSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.apache.wayang.core.util.json.WayangJsonObj;

/**
 * Stores execution data have been collected by the {@link CrossPlatformExecutor}.
 * The current version uses JSON as serialization format.
 */
public class ExecutionLog implements AutoCloseable {

    private final Logger logger = LogManager.getLogger(this.getClass());

    /**
     * Path to the repository file.
     */
    private final String repositoryPath;

    /**
     * {@link Configuration} to use.
     */
    private final Configuration configuration;

    /**
     * Created on demand an can be closed as well.
     */
    private BufferedWriter writer;

    private ExecutionLog(Configuration configuration, String repositoryPath) {
        this.configuration = configuration;
        this.repositoryPath = repositoryPath;
        this.logger.info("Curating execution log at {}.", repositoryPath);
    }

    /**
     * Opens an instance according to the {@link Configuration}.
     *
     * @param configuration describes the instance to be opened
     * @return the new instance
     */
    public static ExecutionLog open(Configuration configuration) {
        return open(configuration, configuration.getStringProperty("wayang.core.log.executions"));
    }


    /**
     * Opens an instance.
     *
     * @param configuration  describes the instance to be opened
     * @param repositoryPath location of the instance
     * @return the new instance
     */
    public static ExecutionLog open(Configuration configuration, String repositoryPath) {
        return new ExecutionLog(configuration, repositoryPath);
    }

    /**
     * Stores the given {@link PartialExecution}s in this instance.
     *
     * @param partialExecutions that should be stored
     */
    public void storeAll(Iterable<PartialExecution> partialExecutions) throws IOException {
        final PartialExecution.Serializer serializer = new PartialExecution.Serializer(this.configuration);
        for (PartialExecution partialExecution : partialExecutions) {
            this.store(partialExecution, serializer);
        }
    }

    /**
     * Stores the given {@link PartialExecution} in this instance.
     *
     * @param partialExecution that should be stored
     */
    public void store(PartialExecution partialExecution) throws IOException {
        this.store(partialExecution, new PartialExecution.Serializer(this.configuration));
    }

    /**
     * Stores the given {@link PartialExecution} in this instance.
     *
     * @param partialExecution               that should be stored
     * @param partialExecutionJsonSerializer serializes {@link PartialExecution}s
     */
    private void store(PartialExecution partialExecution, JsonSerializer<PartialExecution> partialExecutionJsonSerializer)
            throws IOException {
        this.write(JsonSerializables.serialize(partialExecution, false, partialExecutionJsonSerializer));
    }

    /**
     * Writes the measuremnt to the {@link #repositoryPath}.
     */
    private void write(WayangJsonObj jsonMeasurement) throws IOException {
        jsonMeasurement.write(this.getWriter());
        writer.write('\n');
    }

    /**
     * Streams the contents of this instance.
     *
     * @return a {@link Stream} of the contained {@link PartialExecution}s
     * @throws IOException
     */
    public Stream<PartialExecution> stream() throws IOException {
        IOUtils.closeQuietly(this.writer);
        this.writer = null;
        final PartialExecution.Serializer serializer = new PartialExecution.Serializer(this.configuration);
        return Files.lines(Paths.get(this.repositoryPath), Charset.forName("UTF-8"))
                .map(line -> {
                    try {
                        return JsonSerializables.deserialize(new WayangJsonObj(line), serializer, PartialExecution.class);
                    } catch (Exception e) {
                        throw new WayangException(String.format("Could not parse \"%s\".", line), e);
                    }
                });
    }

    /**
     * Initializes the {@link #writer} if it does not exist currently.
     *
     * @return the {@link #writer}
     */
    private BufferedWriter getWriter() throws FileNotFoundException, UnsupportedEncodingException {
        if (this.writer != null) {
            return this.writer;
        }

        try {
            File file = new File(this.repositoryPath);
            final File parentFile = file.getParentFile();
            if (!parentFile.exists() && !file.getParentFile().mkdirs()) {
                throw new WayangException("Could not initialize cardinality repository.");
            }
            return this.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8"));
        } catch (WayangException e) {
            throw e;
        } catch (Exception e) {
            throw new WayangException(String.format("Cannot write to %s.", this.repositoryPath), e);
        }
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(this.writer);
    }
}
