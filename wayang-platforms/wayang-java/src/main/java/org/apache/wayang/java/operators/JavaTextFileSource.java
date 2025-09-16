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

import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is execution operator implements the {@link TextFileSource}.
 */
public class JavaTextFileSource extends TextFileSource implements JavaExecutionOperator {

    private static final Logger logger = LoggerFactory.getLogger(JavaTextFileSource.class);

    /**
     * @return Stream<String> from the provided URL
     */
    public static Stream<String> streamFromURL(final URL sourceUrl) {
        try {
            final HttpURLConnection connection = (HttpURLConnection) sourceUrl.openConnection();
            connection.setRequestMethod("GET");

            // Check if the response code indicates success (HTTP status code 200)
            if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
                logger.info(">>> Ready to stream the data from URL: " + sourceUrl.toString());
                // Read the data line by line and process it in the StreamChannel
                final BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                return reader.lines().onClose(() -> {
                    try {
                        connection.disconnect();
                        reader.close();
                    } catch (final IOException e) {
                        e.printStackTrace();
                    }
                });
            } else {
                throw new WayangException("Connection with Http failed");
            }
        } catch (final Exception e) {
            throw new WayangException(e);
        }
    }

    /**
     * @return Stream<String> from the file system
     */
    public static Stream<String> streamFromFs(final String path) {
        try {
            return Files.lines(Path.of(URI.create(path)));
        } catch (final Exception e) {
            throw new WayangException(e);
        }
    }

    public JavaTextFileSource(final String inputUrl) {
        super(inputUrl);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaTextFileSource(final TextFileSource that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final JavaExecutor javaExecutor,
            final OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final String urlStr = this.getInputUrl().trim();
        final URL sourceUrl;

        try {
            sourceUrl = new URL(urlStr);
        } catch (final Exception e) {
            throw new WayangException("Could not create URL from string: " + urlStr, e);
        }

        final String protocol = sourceUrl.getProtocol();

        final Stream<String> lines = (protocol.startsWith("https") || protocol.startsWith("http"))
                ? JavaTextFileSource.streamFromURL(sourceUrl)
                : JavaTextFileSource.streamFromFs(urlStr);

        ((StreamChannel.Instance) outputs[0]).accept(lines);

        final ExecutionLineageNode prepareLineageNode = new ExecutionLineageNode(operatorContext);
        prepareLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.textfilesource.load.prepare", javaExecutor.getConfiguration()));

        final ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);
        mainLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.textfilesource.load.main", javaExecutor.getConfiguration()));

        outputs[0].getLineage().addPredecessor(mainLineageNode);

        return prepareLineageNode.collectAndMark();
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("wayang.java.textfilesource.load.prepare", "wayang.java.textfilesource.load.main");
    }

    @Override
    public JavaTextFileSource copy() {
        return new JavaTextFileSource(this.getInputUrl());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
