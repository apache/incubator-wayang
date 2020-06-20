package org.qcri.rheem.giraph.operators;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.PageRankOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.giraph.Algorithm.PageRankAlgorithm;
import org.qcri.rheem.giraph.Algorithm.PageRankParameters;
import org.qcri.rheem.giraph.execution.GiraphExecutor;
import org.qcri.rheem.giraph.platform.GiraphPlatform;
import org.qcri.rheem.java.channels.StreamChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Stream;

/**
 * PageRank {@link Operator} implementation for the {@link GiraphPlatform}.
 */
public class GiraphPageRankOperator extends PageRankOperator implements GiraphExecutionOperator {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

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
            throw new RheemException(String.format("Running %s failed.", this), e);
        } catch (URISyntaxException e) {
            throw new RheemException(e);
        } catch (InterruptedException e) {
            throw new RheemException(e);
        } catch (ClassNotFoundException e) {
            throw new RheemException(e);
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
                () -> new RheemException(String.format("Cannot access file system of %s.", tempDirPath))
        );
        //delete the file the output if exist
        fs.delete(tempDirPath, true);

        final String inputPath = inputFileChannelInstance.getSinglePath();

        GiraphConfiguration conf = giraphExecutor.getGiraphConfiguration();
        //vertex reader
        conf.set("giraph.vertex.input.dir", inputPath);
        conf.set("mapred.job.tracker", configuration.getStringProperty("rheem.giraph.job.tracker"));
        conf.set("mapreduce.job.counters.limit", configuration.getStringProperty("rheem.mapreduce.job.counters.limit"));
        conf.setWorkerConfiguration((int)configuration.getLongProperty("rheem.giraph.maxWorkers"),
                (int)configuration.getLongProperty("rheem.giraph.minWorkers"),
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
        conf.setNumComputeThreads((int)configuration.getLongProperty("rheem.giraph.numThread"));

        conf.setVertexOutputFormatClass(PageRankAlgorithm.PageRankVertexOutputFormat.class);

        GiraphJob job = new GiraphJob(conf, "rheem-giraph");
        job.run(true);

        final String actualInputPath = FileSystems.findActualSingleInputPath(tempDirPath);
        Stream<Tuple2<Long, Float>> stream = this.createStream(actualInputPath);

        outputChannelInstance.accept(stream);

        final ExecutionLineageNode mainExecutionLineage = new ExecutionLineageNode(operatorContext);
        mainExecutionLineage.add(LoadProfileEstimators.createFromSpecification(
                "rheem.giraph.pagerank.load.main", configuration
        ));
        mainExecutionLineage.addPredecessor(inputFileChannelInstance.getLineage());

        final ExecutionLineageNode outputExecutionLineage = new ExecutionLineageNode(operatorContext);
        outputExecutionLineage.add(LoadProfileEstimators.createFromSpecification(
                "rheem.giraph.pagerank.load.output", configuration
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
        return Arrays.asList("rheem.giraph.pagerank.load.main", "rheem.giraph.pagerank.load.output");
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
            path = configuration.getStringProperty("rheem.giraph.hdfs.tempdir");
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
        return org.qcri.rheem.core.util.fs.FileUtils.streamLines(path).map(line -> {
            String[] part = line.split("\t");
            return new Tuple2<>(Long.parseLong(part[0]), Float.parseFloat(part[1]));
        });
    }




}
