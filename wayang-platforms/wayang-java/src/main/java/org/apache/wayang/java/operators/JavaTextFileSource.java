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
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is execution operator implements the {@link TextFileSource}.
 */
public class JavaTextFileSource extends TextFileSource implements JavaExecutionOperator {

    private static final Logger logger = LoggerFactory.getLogger(JavaTextFileSource.class);

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

        try {
            final URL sourceUrl = new URL(urlStr);
            final String protocol = sourceUrl.getProtocol();
   
            final InputStream inputStream = protocol.startsWith("https") || protocol.startsWith("http")
                    ? sourceUrl.openConnection().getInputStream()
                    : FileSystems.getFileSystem(urlStr).get().open(urlStr);

            final InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            final BufferedReader reader = new BufferedReader(inputStreamReader);

            ((StreamChannel.Instance) outputs[0]).accept(reader.lines().parallel());
        } catch (final MalformedURLException e) {
            throw new RuntimeException(e);
        } catch (final ProtocolException e) {
            throw new RuntimeException(e);
        } catch (final IOException e) {
            throw new WayangException(String.format("Reading %s failed.", urlStr), e);
        }

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
