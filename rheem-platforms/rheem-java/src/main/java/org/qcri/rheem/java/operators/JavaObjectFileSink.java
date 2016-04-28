package org.qcri.rheem.java.operators;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * {@link Operator} for the {@link JavaPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see JavaObjectFileSource
 */
public class JavaObjectFileSink<T> extends UnarySink<T> implements JavaExecutionOperator {

    private FileChannel.Instance outputChannelInstance;

    private final String targetPath;

    public JavaObjectFileSink(String targetPath, DataSetType type) {
        super(type, null);
        this.targetPath = targetPath;
    }

    @Override
    public void evaluate(JavaChannelInstance[] inputs, JavaChannelInstance[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();

        // Prepare Hadoop's SequenceFile.Writer.
        final SequenceFile.Writer.Option fileOption = SequenceFile.Writer.file(new Path(this.targetPath));
        final SequenceFile.Writer.Option keyClassOption = SequenceFile.Writer.keyClass(NullWritable.class);
        final SequenceFile.Writer.Option valueClassOption = SequenceFile.Writer.valueClass(BytesWritable.class);
        try (SequenceFile.Writer writer = SequenceFile.createWriter(new Configuration(true), fileOption, keyClassOption, valueClassOption)) {

            // Chunk the stream of data quanta and write the chunks into the sequence file.
            StreamChunker streamChunker = new StreamChunker(10, (chunk, size) -> {
                if (chunk.length != size) {
                    chunk = Arrays.copyOfRange(chunk, 0, size);
                }
                try {
                    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    final ObjectOutputStream oos = new ObjectOutputStream(bos);
                    oos.writeObject(chunk);
                    BytesWritable bytesWritable = new BytesWritable(bos.toByteArray());
                    writer.append(NullWritable.get(), bytesWritable);
                } catch (IOException e) {
                    throw new UncheckedIOException("Writing or serialization failed.", e);
                }
            });
            inputs[0].provideStream().forEach(streamChunker::push);
            streamChunker.fire();
            if (this.outputChannelInstance != null) {
                this.outputChannelInstance.setMeasuredCardinality(streamChunker.numPushedObjects);
            }
            LoggerFactory.getLogger(this.getClass()).info("Writing dataset to {}.", this.targetPath);
        } catch (IOException | UncheckedIOException e) {
            throw new RheemException("Could not write stream to sequence file.", e);
        }
    }


    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        // NB: Not actually measured.
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(1, 0, .8d, (inputCards, outputCards) -> 2000 * inputCards[0] + 810000),
                new DefaultLoadEstimator(1, 0, 0, (inputCards, outputCards) -> 0),
                new DefaultLoadEstimator(1, 0, 0, (inputCards, outputCards) -> inputCards[0] * 256),
                new DefaultLoadEstimator(1, 0, 0, (inputCards, outputCards) -> 0)
        );
        return Optional.of(mainEstimator);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaObjectFileSink<>(this.targetPath, this.getType());
    }

    @Override
    public void instrumentSink(JavaChannelInstance channelExecutor) {
        this.outputChannelInstance = (FileChannel.Instance) channelExecutor;
    }

    public String getTargetPath() {
        return this.targetPath;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Collections.singletonList(FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR);
    }

    /**
     * Utility to chunk a {@link Stream} into portions of fixed size.
     */
    public static class StreamChunker {

        private final BiConsumer<Object[], Integer> action;

        private final Object[] chunk;

        private int nextIndex;

        private long numPushedObjects = 0L;

        public StreamChunker(int chunkSize, BiConsumer<Object[], Integer> action) {
            this.action = action;
            this.chunk = new Object[chunkSize];
            this.nextIndex = 0;
        }

        /**
         * Add a new element to the current chunk. Fire, if the chunk is complete.
         */
        public void push(Object obj) {
            this.numPushedObjects++;
            this.chunk[this.nextIndex] = obj;
            if (++this.nextIndex >= this.chunk.length) {
                this.fire();
            }
        }

        /**
         * Apply the specified {@link #action} to the current chunk and prepare for a new chunk.
         */
        public void fire() {
            if (this.nextIndex > 0) {
                this.action.accept(this.chunk, this.nextIndex);
                this.nextIndex = 0;
            }
        }


    }
}
