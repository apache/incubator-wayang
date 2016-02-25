package org.qcri.rheem.spark.channels;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.operators.SparkObjectFileSink;
import org.qcri.rheem.spark.operators.SparkObjectFileSource;
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
 * Sets up {@link FileChannel} usage in the {@link SparkPlatform}.
 */
public class HdfsFileInitializer implements ChannelInitializer {

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
    public Channel setUpOutput(ChannelDescriptor fileDescriptor, ExecutionTask sourceTask, int index) {
        // Set up an internal Channel at first.
        final ChannelInitializer internalChannelInitializer = sourceTask
                .getOperator()
                .getPlatform()
                .getChannelManager()
                .getChannelInitializer(RddChannel.DESCRIPTOR);
        assert internalChannelInitializer != null;
        final Channel internalChannel = internalChannelInitializer.setUpOutput(RddChannel.DESCRIPTOR, sourceTask, index);

        // Create a sink to write the HDFS file.
        ExecutionTask sinkTask = this.setUpSparkObjectFileSink(sourceTask, index, internalChannel);

        // Check if the final FileChannel already exists.
        assert sinkTask.getOutputChannels().length == 1;
        if (sinkTask.getOutputChannel(0) != null) {
            assert sinkTask.getOutputChannel(0) instanceof FileChannel;
            return sinkTask.getOutputChannel(0);
        }

        // Create the actual FileChannel.
        final FileChannel fileChannel = new FileChannel((FileChannel.Descriptor) fileDescriptor,
                sinkTask, index, Channel.extractCardinalityEstimate(sourceTask, index));
        fileChannel.addPath(((SparkObjectFileSink<?>) sinkTask.getOperator()).getTargetPath());
        fileChannel.addSibling(internalChannel);
        return fileChannel;
    }

    private ExecutionTask setUpSparkObjectFileSink(ExecutionTask sourceTask, int outputIndex, Channel internalChannel) {
        // Check if the Channel already is consumed by a JavaObjectFileSink.
        for (ExecutionTask consumerTask : internalChannel.getConsumers()) {
            if (consumerTask.getOperator() instanceof SparkObjectFileSink<?>) {
                return consumerTask;
            }
        }

        // Create the SparkObjectFileSink.
        final DataSetType<?> dataSetType = sourceTask.getOperator().getOutput(outputIndex).getType();
        final String targetPath = pickTempPath();
        SparkObjectFileSink<?> sparkObjectFileSink = new SparkObjectFileSink<>(targetPath, dataSetType);
        sparkObjectFileSink.getInput(0).setCardinalityEstimate(Channel.extractCardinalityEstimate(sourceTask, outputIndex));
        ExecutionTask sinkTask = new ExecutionTask(sparkObjectFileSink, sparkObjectFileSink.getNumInputs(), 1);

        // Connect it to the internalChannel.
        final ChannelInitializer channelInitializer = sparkObjectFileSink
                .getPlatform()
                .getChannelManager()
                .getChannelInitializer(internalChannel.getDescriptor());
        channelInitializer.setUpInput(internalChannel, sinkTask, 0);

        return sinkTask;
    }

    @Override
    public void setUpInput(Channel channel, ExecutionTask targetTask, int inputIndex) {
        FileChannel fileChannel = (FileChannel) channel;
        assert fileChannel.getPaths().size() == 1 : "We support only single HDFS files so far.";

        // NB: We always put the HDFS file contents into a Collection. That's not necessary if we don't broadcast
        // and use it only once.

        // Intercept with a SparkObjectFileSource.
        // TODO: Improve management of data types, file paths, serialization formats etc.
        ExecutionTask sourceTask = this.setUpSparkObjectFileSource(fileChannel);

        // Set up the actual input..
        final ChannelInitializer internalChannelInitializer = SparkPlatform.getInstance().getChannelManager()
                .getChannelInitializer(RddChannel.DESCRIPTOR);
        assert internalChannelInitializer !=  null;
        final Channel internalChannel = internalChannelInitializer.setUpOutput(RddChannel.DESCRIPTOR, sourceTask, 0);
        internalChannel.addSibling(fileChannel);
        internalChannelInitializer.setUpInput(internalChannel, targetTask, inputIndex);
    }

    private ExecutionTask setUpSparkObjectFileSource(FileChannel fileChannel) {
        // Check if there is already is a SparkObjectFileSource in place.
        for (ExecutionTask consumerTask : fileChannel.getConsumers()) {
            if (consumerTask.getOperator() instanceof SparkObjectFileSource<?>) {
                return consumerTask;
            }
        }

        // Create the SparkObjectFileSink.
        // FIXME: This is neither elegant nor sound, as we make assumptions on the FileChannel producer.
        final DataSetType<?> dataSetType = fileChannel.getProducer().getOperator().getInput(0).getType();
        SparkObjectFileSource<?> sparkObjectFileSource = new SparkObjectFileSource<>(fileChannel.getSinglePath(), dataSetType);
        sparkObjectFileSource.getOutput(0).setCardinalityEstimate(fileChannel.getCardinalityEstimate());
        ExecutionTask sourceTask = new ExecutionTask(sparkObjectFileSource, 1, sparkObjectFileSource.getNumOutputs());
        fileChannel.addConsumer(sourceTask, 0);

        return sourceTask;
    }

    @Override
    public boolean isReusable() {
        return true;
    }

    @Override
    public boolean isInternal() {
        return false;
    }

    public static class Executor implements ChannelExecutor {

        private final FileChannel fileChannel;

        private boolean wasTriggered = false;

        public Executor(FileChannel fileChannel) {
            this.fileChannel = fileChannel;
        }

        @Override
        public void acceptRdd(JavaRDD<?> rdd) throws RheemException {
            assert rdd == null;
            this.wasTriggered = true;
        }

        @Override
        public void acceptBroadcast(Broadcast broadcast) {
            throw new RuntimeException("Does not accept broadcasts.");
        }

        @Override
        public <T> JavaRDD<T> provideRdd() {
            return null;
        }

        @Override
        public <T> Broadcast<T> provideBroadcast() {
            throw new RuntimeException("Does not provide broadcasts.");
        }

        @Override
        public void dispose() {
            for (String path : this.fileChannel.getPaths()) {
                try {
                    // TODO: delete HDFS files
                    final Path pathToDelete = Paths.get(new URI(path));
                    Files.delete(pathToDelete);
                } catch (URISyntaxException | IOException e) {
                    LoggerFactory.getLogger(this.getClass()).error("Could not delete {}.", path);
                }
            }
        }

        @Override
        public long getCardinality() throws RheemException {
            return -1;
        }

        @Override
        public boolean ensureExecution() {
            return this.wasTriggered;
        }
    }
}
