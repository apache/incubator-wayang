package org.qcri.rheem.java.channels;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.basic.channels.HdfsFile;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.operators.JavaObjectFileSink;
import org.qcri.rheem.java.operators.JavaObjectFileSource;
import org.qcri.rheem.java.JavaPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.stream.Stream;

/**
 * Sets up {@link HdfsFile} usage in the {@link JavaPlatform}.
 */
public class HdfsFileInitializer implements ChannelInitializer {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static String pickTempPath() {
        // TODO: Do this properly via the configuration.
        try {
            final Path tempDirectory = Files.createTempDirectory("rheem-java");
            Path tempPath = tempDirectory.resolve("data");
            return tempPath.toUri().toString();
        } catch (IOException e) {
            throw new RheemException(e);
        }
    }

    @Override
    public Channel setUpOutput(ExecutionTask sourceTask, int index) {
        // Set up an internal Channel at first.
        final ChannelInitializer streamChannelInitializer = sourceTask.getOperator().getPlatform()
                .getChannelManager().getChannelInitializer(StreamChannel.class);
        assert streamChannelInitializer != null;
        final Channel internalChannel = streamChannelInitializer.setUpOutput(sourceTask, index);

        // Create a sink to write the HDFS file.
        ExecutionTask sinkTask = this.setUpJavaObjectFileSink(sourceTask, index, internalChannel);

        // Check if the final HdfsFile already exists.
        assert sinkTask.getOutputChannels().length == 1;
        if (sinkTask.getOutputChannel(0) != null) {
            assert sinkTask.getOutputChannel(0) instanceof HdfsFile :
                    String.format("Expected %s, found %s.", HdfsFile.class.getSimpleName(), sinkTask.getOutputChannel(0));
            return sinkTask.getOutputChannel(0);
        }

        // Create the actual HdfsFile.
        final HdfsFile hdfsFile = new HdfsFile(sinkTask, index, Channel.extractCardinalityEstimate(sourceTask, index));
        hdfsFile.addPath(((JavaObjectFileSink<?>) sinkTask.getOperator()).getTargetPath());
        return hdfsFile;
    }

    private ExecutionTask setUpJavaObjectFileSink(ExecutionTask sourceTask, int outputIndex, Channel internalChannel) {
        // Check if the Channel already is consumed by a JavaObjectFileSink.
        for (ExecutionTask consumerTask : internalChannel.getConsumers()) {
            if (consumerTask.getOperator() instanceof JavaObjectFileSink<?>) {
                return consumerTask;
            }
        }

        // Create the JavaObjectFileSink.
        final DataSetType<?> dataSetType = sourceTask.getOperator().getOutput(outputIndex).getType();
        final String targetPath = pickTempPath();
        JavaObjectFileSink<?> javaObjectFileSink = new JavaObjectFileSink<>(targetPath, dataSetType);
        javaObjectFileSink.getInput(0).setCardinalityEstimate(Channel.extractCardinalityEstimate(sourceTask, outputIndex));
        ExecutionTask sinkTask = new ExecutionTask(javaObjectFileSink, javaObjectFileSink.getNumInputs(), 1);

        // Connect it to the internalChannel.
        final ChannelInitializer channelInitializer = javaObjectFileSink
                .getPlatform()
                .getChannelManager()
                .getChannelInitializer(internalChannel.getClass());
        channelInitializer.setUpInput(internalChannel, sinkTask, 0);

        return sinkTask;
    }

    @Override
    public void setUpInput(Channel channel, ExecutionTask targetTask, int inputIndex) {
        HdfsFile hdfsFile = (HdfsFile) channel;
        assert hdfsFile.getPaths().size() == 1 : "We support only single HDFS files so far.";
        final String targetPath = hdfsFile.getPaths().stream().findAny().get();

        // NB: We always put the HDFS file contents into a Collection. That's not necessary if we don't broadcast
        // and use it only once.

        // Intercept with a JavaObjectFileSource.
        // TODO: Improve management of data types, file paths, serialization formats etc.
        ExecutionTask sourceTask = this.setUpJavaObjectFileSource(hdfsFile);

        // Set up the actual input..
        final ChannelInitializer internalChannelInitializer = targetTask.getOperator().getPlatform().getChannelManager()
                .getChannelInitializer(CollectionChannel.class);
        Validate.notNull(internalChannelInitializer);
        final Channel internalChannel = internalChannelInitializer.setUpOutput(sourceTask, 0);
        internalChannelInitializer.setUpInput(internalChannel, targetTask, inputIndex);
    }

    private ExecutionTask setUpJavaObjectFileSource(HdfsFile hdfsFile) {
        // Check if there is already is a JavaObjectFileSource in place.
        for (ExecutionTask consumerTask : hdfsFile.getConsumers()) {
            if (consumerTask.getOperator() instanceof JavaObjectFileSource<?>) {
                return consumerTask;
            }
        }

        // Create the JavaObjectFileSink.
        // FIXME: This is neither elegant nor sound, as we make assumptions on the HdfsFile producer.
        final DataSetType<?> dataSetType = hdfsFile.getProducer().getOperator().getInput(0).getType();
        JavaObjectFileSource<?> javaObjectFileSource = new JavaObjectFileSource<>(hdfsFile.getSinglePath(), dataSetType);
        javaObjectFileSource.getOutput(0).setCardinalityEstimate(hdfsFile.getCardinalityEstimate());
        ExecutionTask sourceTask = new ExecutionTask(javaObjectFileSource, 1, javaObjectFileSource.getNumOutputs());
        hdfsFile.addConsumer(sourceTask, 0);

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

        private final HdfsFile hdfsFile;

        public Executor(HdfsFile hdfsFile) {
            this.hdfsFile = hdfsFile;
        }

        @Override
        public void acceptStream(Stream<?> stream) {
            assert stream == null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Stream<?> provideStream() {
            return null;
        }

        @Override
        public void acceptCollection(Collection<?> collection) {
            assert collection == null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Collection<?> provideCollection() {
            return null;
        }

        @Override
        public boolean canProvideCollection() {
            return true;
        }
    }
}
