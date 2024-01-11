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
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.stream.Stream;

/**
 * This is execution operator implements the {@link TextFileSource}.
 */
public class JavaTextFileSource extends TextFileSource implements JavaExecutionOperator {

    public JavaTextFileSource(String inputUrl) {
        super(inputUrl);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaTextFileSource(TextFileSource that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        String urlStr = this.getInputUrl().trim();

        try {

            URL sourceUrl = new URL( urlStr );
            String protocol = sourceUrl.getProtocol();

            if ( protocol.startsWith("https") || protocol.startsWith("http")  ) {
                try {
                    HttpURLConnection connection = (HttpURLConnection) sourceUrl.openConnection();
                    connection.setRequestMethod("GET");
                    // Check if the response code indicates success (HTTP status code 200)
                    if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
                        System.out.println(">>> Ready to stream the data from URL: " + urlStr);
                        // Read the data line by line and process it in the StreamChannel
                        Stream<String> lines2 = new BufferedReader(new InputStreamReader(connection.getInputStream())).lines();
                        ((StreamChannel.Instance) outputs[0]).accept(lines2);
                    }
                }
                catch (IOException ioException) {
                    ioException.printStackTrace();
                    throw new WayangException(String.format("Reading from URL: %s failed.", urlStr), ioException);
                }
            }
            else if ( sourceUrl.getProtocol().startsWith("file") ) {

                FileSystem fs = FileSystems.getFileSystem(urlStr).orElseThrow(
                        () -> new WayangException(String.format("Cannot access file system of %s. ", urlStr))
                );

                try {
                    final InputStream inputStream = fs.open(urlStr);
                    Stream<String> lines = new BufferedReader(new InputStreamReader(inputStream)).lines();
                    ((StreamChannel.Instance) outputs[0]).accept(lines);
                }
                catch (IOException ioException) {
                    ioException.printStackTrace();
                    throw new WayangException(String.format("Reading from FILE: %s failed.", urlStr), ioException);
                }
            }
            else {
                throw new WayangException(String.format("PROTOCOL NOT SUPPORTED IN JavaTextFileSource. [%s] [%s] SUPPORTED ARE: (file|http|https)", urlStr, protocol));
            }
        }
        catch (MalformedURLException e) {
            e.printStackTrace();
            throw new WayangException(String.format("Provided URL is not a valid URL: %s (MalformedURLException)", urlStr), e);
        }

        ExecutionLineageNode prepareLineageNode = new ExecutionLineageNode(operatorContext);
        prepareLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.textfilesource.load.prepare", javaExecutor.getConfiguration()
        ));
        ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);
        mainLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.textfilesource.load.main", javaExecutor.getConfiguration()
        ));

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
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
