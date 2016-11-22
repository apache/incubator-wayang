package org.qcri.rheem.java.operators;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * {@link Operator} for the {@link JavaPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see JavaObjectFileSource
 */
public class JavaObjectFileSink<T> extends UnarySink<T> implements JavaExecutionOperator {

    private final String targetPath;

    public JavaObjectFileSink(DataSetType<T> type) {
        this(null, type);
    }

    public JavaObjectFileSink(String targetPath, DataSetType<T> type) {
        super(type);
        this.targetPath = targetPath;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();

        // Prepare Hadoop's SequenceFile.Writer.
        FileChannel.Instance output = (FileChannel.Instance) outputs[0];
        final String path = output.addGivenOrTempPath(this.targetPath, javaExecutor.getCompiler().getConfiguration());

        final SequenceFile.Writer.Option fileOption = SequenceFile.Writer.file(new Path(path));
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
            ((JavaChannelInstance) inputs[0]).provideStream().forEach(streamChunker::push);
            streamChunker.fire();
            LoggerFactory.getLogger(this.getClass()).info("Writing dataset to {}.", path);
        } catch (IOException | UncheckedIOException e) {
            throw new RheemException("Could not write stream to sequence file.", e);
        }

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.objectfilesink.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaObjectFileSink<>(this.targetPath, this.getType());
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
