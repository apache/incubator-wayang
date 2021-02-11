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

package org.apache.wayang.giraph.operators;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.PageRankOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.giraph.Algorithm.PageRankAlgorithm;
import org.apache.wayang.giraph.Algorithm.PageRankParameters;
import org.apache.wayang.giraph.execution.GiraphExecutor;
import org.apache.wayang.giraph.platform.GiraphPlatform;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Stream;

/**
 * PageRank {@link Operator} implementation for the {@link GiraphPlatform}.
 */
public class GiraphPageRankOperator extends PageRankOperator implements GiraphExecutionOperator {

    private final Logger logger = LogManager.getLogger(this.getClass());

    private String path_out;

    public GiraphPageRankOperator(Integer numIterations) {
        super(numIterations);
        setPathOut(null, null);
    }
    public GiraphPageRankOperator(PageRankOperator pageRankOperator) {
        super(pageRankOperator);
        setPathOut(null, null);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> execute(
            ChannelInstance[] inputChannelInstances,
            ChannelInstance[] outputChannelInstances,
            GiraphExecutor giraphExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputChannelInstances.length == this.getNumInputs();
        assert outputChannelInstances.length == this.getNumOutputs();

        final FileChannel.Instance inputChannel = (FileChannel.Instance) inputChannelInstances[0];
        final StreamChannel.Instance outputChanne = (StreamChannel.Instance) outputChannelInstances[0];
        try {
            return this.runGiraph(inputChannel, outputChanne, giraphExecutor, operatorContext);
        } catch (IOException e) {
            throw new WayangException(String.format("Running %s failed.", this), e);
        } catch (URISyntaxException e) {
            throw new WayangException(e);
        } catch (InterruptedException e) {
            throw new WayangException(e);
        } catch (ClassNotFoundException e) {
            throw new WayangException(e);
        }
    }

    private Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> runGiraph(
            FileChannel.Instance inputFileChannelInstance,
            StreamChannel.Instance outputChannelInstance,
            GiraphExecutor giraphExecutor,
            OptimizationContext.OperatorContext operatorContext)
            throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        assert inputFileChannelInstance.wasProduced();
        Configuration configuration = operatorContext.getOptimizationContext().getConfiguration();
        String tempDirPath = this.getPathOut(configuration);

        PageRankParameters.setParameter(PageRankParameters.PageRankEnum.ITERATION, this.getNumIterations());

        FileSystem fs = FileSystems.getFileSystem(tempDirPath).orElseThrow(
                () -> new WayangException(String.format("Cannot access file system of %s.", tempDirPath))
        );
        //delete the file the output if exist
        fs.delete(tempDirPath, true);

        final String inputPath = inputFileChannelInstance.getSinglePath();

        GiraphConfiguration conf = giraphExecutor.getGiraphConfiguration();
        //vertex reader
        conf.set("giraph.vertex.input.dir", inputPath);
        conf.set("mapred.job.tracker", configuration.getStringProperty("wayang.giraph.job.tracker"));
        conf.set("mapreduce.job.counters.limit", configuration.getStringProperty("wayang.mapreduce.job.counters.limit"));
        conf.setWorkerConfiguration((int)configuration.getLongProperty("wayang.giraph.maxWorkers"),
                (int)configuration.getLongProperty("wayang.giraph.minWorkers"),
        100.0f);
        conf.set("giraph.SplitMasterWorker", "false");
        conf.set("mapreduce.output.fileoutputformat.outputdir", tempDirPath);
        conf.setComputationClass(PageRankAlgorithm.class);
        conf.setVertexInputFormatClass(
                PageRankAlgorithm.PageRankVertexInputFormat.class);
        conf.setWorkerContextClass(
                PageRankAlgorithm.PageRankWorkerContext.class);
        conf.setMasterComputeClass(
                PageRankAlgorithm.PageRankMasterCompute.class);
        conf.setNumComputeThreads((int)configuration.getLongProperty("wayang.giraph.numThread"));

        conf.setVertexOutputFormatClass(PageRankAlgorithm.PageRankVertexOutputFormat.class);

        GiraphJob job = new GiraphJob(conf, "wayang-giraph");
        job.run(true);

        final String actualInputPath = FileSystems.findActualSingleInputPath(tempDirPath);
        Stream<Tuple2<Long, Float>> stream = this.createStream(actualInputPath);

        outputChannelInstance.accept(stream);

        final ExecutionLineageNode mainExecutionLineage = new ExecutionLineageNode(operatorContext);
        mainExecutionLineage.add(LoadProfileEstimators.createFromSpecification(
                "wayang.giraph.pagerank.load.main", configuration
        ));
        mainExecutionLineage.addPredecessor(inputFileChannelInstance.getLineage());

        final ExecutionLineageNode outputExecutionLineage = new ExecutionLineageNode(operatorContext);
        outputExecutionLineage.add(LoadProfileEstimators.createFromSpecification(
                "wayang.giraph.pagerank.load.output", configuration
        ));
        outputChannelInstance.getLineage().addPredecessor(outputExecutionLineage);

        return mainExecutionLineage.collectAndMark();
    }


    @Override
    public Platform getPlatform() {
        return GiraphPlatform.getInstance();
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("wayang.giraph.pagerank.load.main", "wayang.giraph.pagerank.load.output");
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_TSV_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }


    public void setPathOut(String path, Configuration configuration){
        if(path == null && configuration != null) {
            path = configuration.getStringProperty("wayang.giraph.hdfs.tempdir");
        }
        this.path_out = path;
    }

    public String getPathOut(Configuration configuration){
        if(this.path_out == null){
            setPathOut(null, configuration);
        }
        return this.path_out;
    }


    private Stream<Tuple2<Long, Float>> createStream(String path) {
        return org.apache.wayang.core.util.fs.FileUtils.streamLines(path).map(line -> {
            String[] part = line.split("\t");
            return new Tuple2<>(Long.parseLong(part[0]), Float.parseFloat(part[1]));
        });
    }




}
