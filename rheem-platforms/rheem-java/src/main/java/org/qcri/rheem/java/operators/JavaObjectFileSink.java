package org.qcri.rheem.java.operators;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.Validate;
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
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.plugin.JavaPlatform;

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

    private final String targetPath;

    public JavaObjectFileSink(String targetPath, DataSetType type) {
        super(type, null);
        this.targetPath = targetPath;
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        Validate.isTrue(inputStreams.length == 1);
        Stream<T> inputStream = (Stream<T>) inputStreams[0];

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
            inputStream.forEach(streamChunker::push);
            streamChunker.fire();
        } catch (IOException | RuntimeIOException e) {
            throw new RheemException("Could not write stream to sequence file.", e);
        }

        return new Stream[0];
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaObjectFileSink<>(this.targetPath, this.getType());
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

        public StreamChunker(int chunkSize, BiConsumer<Object[], Integer> action) {
            this.action = action;
            this.chunk = new Object[chunkSize];
            this.nextIndex = 0;
        }

        /**
         * Add a new element to the current chunk. Fire, if the chunk is complete.
         */
        public void push(Object obj) {
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
