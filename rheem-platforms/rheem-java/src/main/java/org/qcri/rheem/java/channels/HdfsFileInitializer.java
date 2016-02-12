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
import org.qcri.rheem.java.plugin.JavaPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Sets up {@link HdfsFile} usage in the {@link JavaPlatform}.
 */
public class HdfsFileInitializer implements ChannelInitializer<HdfsFile> {
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
    public HdfsFile setUpOutput(ExecutionTask executionTask, int index) {
        // Intercept with a JavaObjectFileSink.
        final ChannelInitializer<StreamChannel> streamChannelInitializer = Channels.getChannelInitializer(StreamChannel.class);
        Validate.notNull(streamChannelInitializer);
        final StreamChannel streamChannel = streamChannelInitializer.setUpOutput(executionTask, index);
        final DataSetType<?> dataSetType = executionTask.getOperator().getOutput(index).getType();
        final String targetPath = pickTempPath();
        JavaObjectFileSink<?> javaObjectFileSink = new JavaObjectFileSink<>(targetPath, dataSetType);
        javaObjectFileSink.getInput(0).setCardinalityEstimate(Channel.extractCardinalityEstimate(executionTask, index));
        ExecutionTask sinkTask = new ExecutionTask(javaObjectFileSink, javaObjectFileSink.getNumInputs(), 1);
        streamChannelInitializer.setUpInput(streamChannel, sinkTask, 0);

        // Create the actual HdfsFile.
        final HdfsFile hdfsFile = new HdfsFile(sinkTask, index, Channel.extractCardinalityEstimate(executionTask, index));
        hdfsFile.addPath(targetPath);
        return hdfsFile;
    }

    @Override
    public void setUpInput(HdfsFile channel, ExecutionTask executionTask, int index) {
        Validate.isTrue(channel.getPaths().size() == 1);
        final String targetPath = channel.getPaths().stream().findAny().get();

        // Intercept with a JavaObjectFileSource.
        // TODO: Improve management of data types, file paths, serialization formats etc.
        final DataSetType<?> dataSetType = channel.getProducer().getOperator().getInput(0).getType();
        JavaObjectFileSource<?> javaObjectFileSource = new JavaObjectFileSource<>(targetPath, dataSetType);
        javaObjectFileSource.getOutput(0).setCardinalityEstimate(channel.getCardinalityEstimate());
        ExecutionTask sourceTask = new ExecutionTask(javaObjectFileSource, 1, javaObjectFileSource.getNumOutputs());
        channel.addConsumer(sourceTask, 0);

        // Set up the actual input..
        final ChannelInitializer<StreamChannel> streamChannelInitializer = Channels.getChannelInitializer(StreamChannel.class);
        Validate.notNull(streamChannelInitializer);
        final StreamChannel streamChannel = streamChannelInitializer.setUpOutput(sourceTask, 0);
        streamChannelInitializer.setUpInput(streamChannel, executionTask, index);
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
        public void acceptStream(Stream<?> stream) {
            Validate.isTrue(stream == null);
        }

        @Override
        public Stream<?> provideStream() {
            return null;
        }

    }
}
