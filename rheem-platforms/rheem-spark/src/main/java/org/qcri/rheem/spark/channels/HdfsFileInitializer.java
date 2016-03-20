package org.qcri.rheem.spark.channels;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.spark.operators.SparkObjectFileSink;
import org.qcri.rheem.spark.operators.SparkObjectFileSource;
import org.qcri.rheem.spark.operators.SparkTsvFileSink;
import org.qcri.rheem.spark.operators.SparkTsvFileSource;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.OptionalLong;

/**
 * Sets up {@link FileChannel} usage in the {@link SparkPlatform}.
 */
public class HdfsFileInitializer implements SparkChannelInitializer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public Tuple<Channel, Channel> setUpOutput(ChannelDescriptor descriptor, OutputSlot<?> outputSlot, OptimizationContext optimizationContext) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public Channel setUpOutput(ChannelDescriptor descriptor, Channel source, OptimizationContext optimizationContext) {
        // Get an RddChannel to operate on.
        final RddChannel rddChannel = this.createRddChannel(source, optimizationContext);

        // Create a sink to write the HDFS file.
        ExecutionTask sinkTask;
        final String targetPath = FileChannel.pickTempPath();
        final String serialization = ((FileChannel.Descriptor) descriptor).getSerialization();
        switch (serialization) {
            case "object-file":
                sinkTask = this.setUpObjectFileSink(rddChannel, targetPath, optimizationContext);
                break;
            case "tsv":
                sinkTask = this.setUpTsvFileSink(rddChannel, targetPath, optimizationContext);
                break;
            default:
                throw new IllegalStateException(String.format("Unsupported serialization: \"%s\"", serialization));
        }
        // Check if the final FileChannel already exists.
        assert sinkTask.getOutputChannels().length == 1;
        if (sinkTask.getOutputChannel(0) != null) {
            assert sinkTask.getOutputChannel(0) instanceof FileChannel;
            return sinkTask.getOutputChannel(0);
        }

        // Create the actual FileChannel.
        final FileChannel fileChannel = new FileChannel((FileChannel.Descriptor) descriptor);
        fileChannel.addPath(targetPath);
        fileChannel.addSibling(source);
        sinkTask.setOutputChannel(0, fileChannel);
        return fileChannel;
    }

    private ExecutionTask setUpObjectFileSink(Channel sourceChannel, String targetPath, OptimizationContext optimizationContext) {
        // Check if the Channel already is consumed by a SparkObjectFileSink.
        for (ExecutionTask consumerTask : sourceChannel.getConsumers()) {
            if (consumerTask.getOperator() instanceof SparkObjectFileSink<?>) {
                this.logger.warn("Unexpected existing HDFS writer.");
                return consumerTask;
            }
        }

        // Create the SparkObjectFileSink.
        final DataSetType<?> dataSetType = sourceChannel.getDataSetType();
        SparkObjectFileSink<?> sparkObjectFileSink = new SparkObjectFileSink<>(targetPath, dataSetType);
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(sparkObjectFileSink);
        operatorContext.setInputCardinality(0, sourceChannel.getCardinalityEstimate(optimizationContext));
        operatorContext.updateTimeEstimate();

        ExecutionTask sinkTask = new ExecutionTask(sparkObjectFileSink, sparkObjectFileSink.getNumInputs(), 1);
        sourceChannel.addConsumer(sinkTask, 0);

        return sinkTask;
    }

    private ExecutionTask setUpTsvFileSink(Channel sourceChannel, String targetPath, OptimizationContext optimizationContext) {
        // Check if the Channel already is consumed by a SparkTsvFileSink.
        for (ExecutionTask consumerTask : sourceChannel.getConsumers()) {
            if (consumerTask.getOperator() instanceof SparkObjectFileSink<?>) {
                this.logger.warn("Unexpected existing HDFS writer.");
                return consumerTask;
            }
        }

        // Create the SparkTsvFileSink.
        final DataSetType<?> dataSetType = sourceChannel.getDataSetType();
        SparkTsvFileSink<?> sparkTsvFileSink = new SparkTsvFileSink<>(targetPath, dataSetType);
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(sparkTsvFileSink);
        operatorContext.setInputCardinality(0, sourceChannel.getCardinalityEstimate(optimizationContext));
        operatorContext.updateTimeEstimate();

        ExecutionTask sinkTask = new ExecutionTask(sparkTsvFileSink, sparkTsvFileSink.getNumInputs(), 1);

        // Connect it to the internalChannel.
        sourceChannel.addConsumer(sinkTask, 0);

        return sinkTask;
    }


    @Override
    public RddChannel provideRddChannel(Channel channel, OptimizationContext optimizationContext) {
        FileChannel fileChannel = (FileChannel) channel;
        assert fileChannel.getPaths().size() == 1 : "We support only single HDFS files so far.";

        // NB: We always put the HDFS file contents into a Collection. That's not necessary if we don't broadcast
        // and use it only once.

        // Intercept with a SparkObjectFileSource.
        ExecutionTask sourceTask;
        final String serialization = fileChannel.getDescriptor().getSerialization();
        switch (serialization) {
            case "object-file":
                sourceTask = this.setUpSparkObjectFileSource(fileChannel);
                break;
            case "tsv":
                sourceTask = this.setUpSparkTsvFileSource(fileChannel);
                break;
            default:
                throw new IllegalStateException(
                        String.format("%s cannot handle \"%s\" serialization.", this, serialization));
        }
        // Set up the actual input..
        final ChannelInitializer internalChannelInitializer = this.getChannelManager()
                .getChannelInitializer(RddChannel.DESCRIPTOR);
        assert internalChannelInitializer != null;
        final Tuple<Channel, Channel> rddChannelSetup = internalChannelInitializer
                .setUpOutput(RddChannel.DESCRIPTOR, sourceTask.getOperator().getOutput(0), optimizationContext);
        fileChannel.addSibling(rddChannelSetup.getField0());
        sourceTask.setOutputChannel(0, rddChannelSetup.getField0());

        return (RddChannel) rddChannelSetup.getField1();
    }

    private ExecutionTask setUpSparkObjectFileSource(FileChannel fileChannel) {
//        // Check if there is already is a SparkObjectFileSource in place.
//        for (ExecutionTask consumerTask : fileChannel.getConsumers()) {
//            if (consumerTask.getOperator() instanceof SparkObjectFileSource<?>) {
//                return consumerTask;
//            }
//        }

        // Create the SparkObjectFileSink.
        final DataSetType<?> dataSetType = fileChannel.getDataSetType();
        SparkObjectFileSource<?> sparkObjectFileSource = new SparkObjectFileSource<>(fileChannel.getSinglePath(), dataSetType);
        ExecutionTask sourceTask = new ExecutionTask(sparkObjectFileSource, 1, sparkObjectFileSource.getNumOutputs());
        fileChannel.addConsumer(sourceTask, 0);

        return sourceTask;
    }

    private ExecutionTask setUpSparkTsvFileSource(FileChannel fileChannel) {
//        // Check if there is already is a SparkObjectFileSource in place.
//        for (ExecutionTask consumerTask : fileChannel.getConsumers()) {
//            if (consumerTask.getOperator() instanceof SparkTsvFileSource<?>) {
//                return consumerTask;
//            }
//        }

        // Create the SparkObjectFileSink.
        final DataSetType<?> dataSetType = fileChannel.getDataSetType();
        SparkTsvFileSource<?> fileSource = new SparkTsvFileSource<>(fileChannel.getSinglePath(), dataSetType);
        ExecutionTask sourceTask = new ExecutionTask(fileSource, 1, fileSource.getNumOutputs());
        fileChannel.addConsumer(sourceTask, 0);

        return sourceTask;
    }

    /**
     * {@link ChannelExecutor} for the {@link FileChannel}.
     */
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
        public void release() {
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
        public OptionalLong getMeasuredCardinality() throws RheemException {
            return OptionalLong.empty();
        }

        @Override
        public boolean ensureExecution() {
            return this.wasTriggered;
        }

        @Override
        public FileChannel getChannel() {
            return this.fileChannel;
        }
    }
}
