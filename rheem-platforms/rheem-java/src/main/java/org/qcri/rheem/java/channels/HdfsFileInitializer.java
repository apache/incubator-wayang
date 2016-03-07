package org.qcri.rheem.java.channels;

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
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.operators.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * Sets up {@link FileChannel} usage in the {@link JavaPlatform}.
 */
public class HdfsFileInitializer implements JavaChannelInitializer {

    @SuppressWarnings("unused")
    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    @Override
    public Tuple<Channel, Channel> setUpOutput(ChannelDescriptor descriptor, OutputSlot<?> outputSlot, OptimizationContext optimizationContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Channel setUpOutput(ChannelDescriptor descriptor, Channel source, OptimizationContext optimizationContext) {
        // Create a sink to write the HDFS file.
        final String targetPath = FileChannel.pickTempPath();

        ExecutionTask sinkTask;
        final FileChannel.Descriptor fileDescriptor = (FileChannel.Descriptor) descriptor;
        final String serialization = fileDescriptor.getSerialization();
        switch (serialization) {
            case "object-file":
                sinkTask = this.setUpJavaObjectFileSink(source, targetPath, optimizationContext);
                break;
            case "tsv":
                sinkTask = this.setUpTsvFileSink(source, targetPath, optimizationContext);
                break;
            default:
                throw new IllegalStateException(String.format("Unsupported serialization: \"%s\"", serialization));
        }

        // Create the actual FileChannel.
        final FileChannel fileChannel = new FileChannel(fileDescriptor);
        fileChannel.addPath(targetPath);
        fileChannel.addSibling(source);
        sinkTask.setOutputChannel(0, fileChannel);
        return fileChannel;
    }


    private ExecutionTask setUpJavaObjectFileSink(Channel sourceChannel, String targetPath, OptimizationContext optimizationContext) {
        // Create the JavaObjectFileSink.
        final DataSetType<?> dataSetType = sourceChannel.getDataSetType();
        JavaObjectFileSink<?> javaObjectFileSink = new JavaObjectFileSink<>(targetPath, dataSetType);
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(javaObjectFileSink);
        operatorContext.setInputCardinality(0, sourceChannel.getCardinalityEstimate(optimizationContext));
        operatorContext.updateTimeEstimate();

        ExecutionTask sinkTask = new ExecutionTask(javaObjectFileSink, javaObjectFileSink.getNumInputs(), 1);
        sourceChannel.addConsumer(sinkTask, 0);

        return sinkTask;
    }

    private ExecutionTask setUpTsvFileSink(Channel sourceChannel, String targetPath, OptimizationContext optimizationContext) {
        // Create the JavaObjectFileSink.
        final DataSetType<?> dataSetType = sourceChannel.getDataSetType();
        JavaTsvFileSink<?> javaTsvFileSink = new JavaTsvFileSink<>(targetPath, dataSetType);
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(javaTsvFileSink);
        operatorContext.setInputCardinality(0, sourceChannel.getCardinalityEstimate(optimizationContext));
        operatorContext.updateTimeEstimate();

        ExecutionTask sinkTask = new ExecutionTask(javaTsvFileSink, javaTsvFileSink.getNumInputs(), 1);
        sourceChannel.addConsumer(sinkTask, 0);

        return sinkTask;
    }

    @Override
    public StreamChannel provideStreamChannel(Channel channel, OptimizationContext optimizationContext) {
        FileChannel fileChannel = (FileChannel) channel;
        assert fileChannel.getPaths().size() == 1 :
                String.format("We support only single files so far (found %d).", fileChannel.getPaths().size());

        // NB: We always put the HDFS file contents into a Collection. That's not necessary if we don't broadcast
        // and use it only once.

        // Create a reader for the file.
        ExecutionTask sourceTask;
        final String serialization = fileChannel.getDescriptor().getSerialization();
        switch (serialization) {
            case "object-file":
                sourceTask = this.setUpJavaObjectFileSource(fileChannel, optimizationContext);
                break;
            case "tsv":
                sourceTask = this.setUpTsvFileSource(fileChannel, optimizationContext);
                break;
            default:
                throw new IllegalStateException(
                        String.format("%s cannot handle \"%s\" serialization.", this.getClass().getSimpleName(), serialization));
        }
        fileChannel.addConsumer(sourceTask, 0);

        // Create and setup the actual StreamChannel.
        final ChannelInitializer internalChannelInitializer = this.getChannelManager()
                .getChannelInitializer(StreamChannel.DESCRIPTOR);
        assert internalChannelInitializer != null;
        final Tuple<Channel, Channel> streamChannelSetup = internalChannelInitializer
                .setUpOutput(StreamChannel.DESCRIPTOR, sourceTask.getOperator().getOutput(0), optimizationContext);
        fileChannel.addSibling(streamChannelSetup.getField0());
        sourceTask.setOutputChannel(0, streamChannelSetup.getField0());

        return (StreamChannel) streamChannelSetup.getField1();
    }

    private ExecutionTask setUpJavaObjectFileSource(FileChannel fileChannel, OptimizationContext optimizationContext) {
        // Check if there is already is a JavaObjectFileSource in place.
        for (ExecutionTask consumerTask : fileChannel.getConsumers()) {
            if (consumerTask.getOperator() instanceof JavaObjectFileSource<?>) {
                return consumerTask;
            }
        }

        // Create the JavaObjectFileSink.
        final DataSetType<?> dataSetType = fileChannel.getDataSetType();
        JavaObjectFileSource<?> javaObjectFileSource = new JavaObjectFileSource<>(fileChannel.getSinglePath(), dataSetType);
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(javaObjectFileSource);
        operatorContext.setOutputCardinality(0, fileChannel.getCardinalityEstimate(optimizationContext));
        operatorContext.updateTimeEstimate();

        ExecutionTask sourceTask = new ExecutionTask(javaObjectFileSource, 1, javaObjectFileSource.getNumOutputs());
        fileChannel.addConsumer(sourceTask, 0);

        return sourceTask;
    }

    private ExecutionTask setUpTsvFileSource(FileChannel fileChannel, OptimizationContext optimizationContext) {
        // Check if there is already is a JavaTsvFileSource in place.
        for (ExecutionTask consumerTask : fileChannel.getConsumers()) {
            if (consumerTask.getOperator() instanceof JavaTsvFileSource<?>) {
                return consumerTask;
            }
        }

        // Create the JavaObjectFileSink.
        final DataSetType<?> dataSetType = fileChannel.getDataSetType();
        JavaTsvFileSource<?> tsvSource = new JavaTsvFileSource<>(fileChannel.getSinglePath(), dataSetType);
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(tsvSource);
        operatorContext.setOutputCardinality(0, fileChannel.getCardinalityEstimate(optimizationContext));
        operatorContext.updateTimeEstimate();

        ExecutionTask sourceTask = new ExecutionTask(tsvSource, 1, tsvSource.getNumOutputs());
        fileChannel.addConsumer(sourceTask, 0);

        return sourceTask;
    }

    /**
     * {@link ChannelExecutor} implementation for the {@link FileChannel}.
     */
    public static class Executor implements ChannelExecutor {

        private final FileChannel fileChannel;

        private boolean isMarkedForInstrumentation;

        private long cardinality = -1;

        private boolean wasTriggered = false;

        public Executor(FileChannel fileChannel) {
            this.fileChannel = fileChannel;
            if (this.fileChannel.isMarkedForInstrumentation()) {
                this.markForInstrumentation();
            }
        }

        @Override
        public void acceptStream(Stream<?> stream) {
            assert stream == null;
            this.wasTriggered = true;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Stream<?> provideStream() {
            assert this.wasTriggered;
            return null;
        }

        @Override
        public void acceptCollection(Collection<?> collection) {
            this.wasTriggered = true;
            assert collection == null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Collection<?> provideCollection() {
            assert this.wasTriggered;
            return null;
        }

        @Override
        public boolean canProvideCollection() {
            return true;
        }

        @Override
        public long getCardinality() throws RheemException {
            assert this.isMarkedForInstrumentation;
            return this.cardinality;
        }

        @Override
        public void markForInstrumentation() {
            this.isMarkedForInstrumentation = true;
            ((JavaExecutionOperator) this.fileChannel.getProducer().getOperator()).instrumentSink(this);
        }

        public void setCardinality(long cardinality) {
            this.cardinality = cardinality;
        }

        @Override
        public boolean ensureExecution() {
            return this.wasTriggered;
        }
    }
}
