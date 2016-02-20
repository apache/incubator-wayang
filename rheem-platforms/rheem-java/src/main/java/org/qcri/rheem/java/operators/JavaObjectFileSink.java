package org.qcri.rheem.java.operators;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.mortbay.io.RuntimeIOException;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.channels.HdfsFileInitializer;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * {@link Operator} for the {@link JavaPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see JavaObjectFileSource
 */
public class JavaObjectFileSink<T> extends UnarySink<T> implements JavaExecutionOperator {

    private HdfsFileInitializer.Executor outputChannelExecutor;

    private final String targetPath;

    public JavaObjectFileSink(String targetPath, DataSetType type) {
        super(type, null);
        this.targetPath = targetPath;
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
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
                    throw new RuntimeIOException("Writing or serialization failed.", e);
                }
            });
            inputs[0].provideStream().forEach(streamChunker::push);
            streamChunker.fire();
            if (this.outputChannelExecutor != null) {
                this.outputChannelExecutor.setCardinality(streamChunker.numPushedObjects);
            }
        } catch (IOException | RuntimeIOException e) {
            throw new RheemException("Could not write stream to sequence file.", e);
        }
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaObjectFileSink<>(this.targetPath, this.getType());
    }

    @Override
    public void instrumentSink(ChannelExecutor channelExecutor) {
        this.outputChannelExecutor = (HdfsFileInitializer.Executor) channelExecutor;
    }

    public String getTargetPath() {
        return this.targetPath;
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
