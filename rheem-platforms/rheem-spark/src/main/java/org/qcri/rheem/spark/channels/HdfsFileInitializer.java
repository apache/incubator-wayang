package org.qcri.rheem.spark.channels;

import org.apache.commons.lang3.Validate;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.basic.channels.HdfsFile;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.operators.SparkSequenceFileSink;
import org.qcri.rheem.spark.operators.SparkSequenceFileSource;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Sets up {@link HdfsFile} usage in the {@link SparkPlatform}.
 */
public class HdfsFileInitializer implements ChannelInitializer<HdfsFile> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static String pickTempPath() {
        // TODO: Do this properly via the configuration.
        try {
            final Path tempDirectory = Files.createTempDirectory("rheem-spark");
            Path tempPath = tempDirectory.resolve("data");
            return tempPath.toUri().toString();
        } catch (IOException e) {
            throw new RheemException(e);
        }
    }

    @Override
    public HdfsFile setUpOutput(ExecutionTask executionTask, int index) {
        // Intercept with a SparkSequenceFileSink.
        final ChannelInitializer<RddChannel> rddChannelInitializer = Channels.getChannelInitializer(RddChannel.class);
        Validate.notNull(rddChannelInitializer);
        final RddChannel rddChannel = rddChannelInitializer.setUpOutput(executionTask, index);
        final DataSetType<?> dataSetType = executionTask.getOperator().getOutput(index).getType();
        final String targetPath = pickTempPath();
        SparkSequenceFileSink<?> sparkSequenceFileSink = new SparkSequenceFileSink<>(targetPath, dataSetType);
        ExecutionTask sinkTask = new ExecutionTask(sparkSequenceFileSink, sparkSequenceFileSink.getNumInputs(), 1);
        rddChannelInitializer.setUpInput(rddChannel, sinkTask, 0);

        // Create the actual HdfsFile.
        final HdfsFile hdfsFile = new HdfsFile(sinkTask, index);
        hdfsFile.addPath(targetPath);
        return hdfsFile;
    }

    @Override
    public void setUpInput(HdfsFile channel, ExecutionTask executionTask, int index) {
        Validate.isTrue(channel.getPaths().size() == 1);
        final String targetPath = channel.getPaths().stream().findAny().get();

        // Intercept with a SparkSequenceFileSource.
        // TODO: Improve management of data types, file paths, serialization formats etc.
        final DataSetType<?> dataSetType = channel.getProducer().getOperator().getInput(0).getType();
        SparkSequenceFileSource<?> sparkSequenceFileSource = new SparkSequenceFileSource<>(targetPath, dataSetType);
        ExecutionTask sourceTask = new ExecutionTask(sparkSequenceFileSource, 1, sparkSequenceFileSource.getNumOutputs());
        channel.addConsumer(sourceTask, 0);

        // Set up the actual input..
        final ChannelInitializer<RddChannel> rddChannelInitializer = Channels.getChannelInitializer(RddChannel.class);
        Validate.notNull(rddChannelInitializer);
        final RddChannel rddChannel = rddChannelInitializer.setUpOutput(sourceTask, 0);
        rddChannelInitializer.setUpInput(rddChannel, executionTask, index);
    }

    @Override
    public boolean isReusable() {
        return true;
    }

    public static class Executor implements ChannelExecutor {

        private final HdfsFile hdfsFile;

        public Executor(HdfsFile hdfsFile) {
            this.hdfsFile = hdfsFile;
        }

        @Override
        public void acceptRdd(JavaRDDLike rdd) {
            Validate.isTrue(rdd == null);
        }

        @Override
        public JavaRDDLike provideRdd() {
            return null;
        }

        @Override
        public void dispose() {
            for (String path : hdfsFile.getPaths()) {
                try {
                    // TODO: delete HDFS files
                    final Path pathToDelete = Paths.get(new URI(path));
                    Files.delete(pathToDelete);
                } catch (URISyntaxException | IOException e) {
                    LoggerFactory.getLogger(this.getClass()).error("Could not delete {}.", path);
                }
            }
        }
    }
}
