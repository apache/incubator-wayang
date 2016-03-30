package org.qcri.rheem.java.operators;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.JavaPlatform;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link Operator} for the {@link JavaPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see JavaObjectFileSink
 */
public class JavaObjectFileSource<T> extends UnarySource<T> implements JavaExecutionOperator {

    private final String sourcePath;

    public JavaObjectFileSource(String sourcePath, DataSetType type) {
        super(type, null);
        this.sourcePath = sourcePath;
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert outputs.length == this.getNumOutputs();

        SequenceFileIterator sequenceFileIterator;
        try {
            final String actualInputPath = FileSystems.findActualSingleInputPath(this.sourcePath);
            sequenceFileIterator = new SequenceFileIterator<>(actualInputPath);
            Stream<?> sequenceFileStream =
                    StreamSupport.stream(Spliterators.spliteratorUnknownSize(sequenceFileIterator, 0), false);
            outputs[0].acceptStream(sequenceFileStream);
        } catch (IOException e) {
            throw new RheemException(String.format("%s failed to read from %s.", this, this.sourcePath), e);
        }
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        final OptionalLong optionalFileSize = FileSystems.getFileSize(this.sourcePath);
        if (!optionalFileSize.isPresent()) {
            LoggerFactory.getLogger(JavaTextFileSource.class).warn("Could not determine file size for {}.", this.sourcePath);
        }
        // NB: Not actually measured!
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(0, 1, .99d, (inputCards, outputCards) -> 1500 * outputCards[0] + 1400000),
                optionalFileSize.isPresent() ?
                        new DefaultLoadEstimator(0, 1, 1d, (inputCards, outputCards) -> optionalFileSize.getAsLong()) :
                        new DefaultLoadEstimator(0, 1, .5d, (inputCards, outputCards) -> outputCards[0] * 100L)
        );
        return Optional.of(mainEstimator);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaObjectFileSource<>(this.sourcePath, this.getType());
    }

    public static class SequenceFileIterator<T> implements Iterator<T>, AutoCloseable, Closeable {

        private SequenceFile.Reader sequenceFileReader;

        private final NullWritable nullWritable = NullWritable.get();

        private final BytesWritable bytesWritable = new BytesWritable();

        private Object[] nextElements;

        private int nextIndex;

        public SequenceFileIterator(String path) throws IOException {
            final SequenceFile.Reader.Option fileOption = SequenceFile.Reader.file(new Path(path));
            this.sequenceFileReader = new SequenceFile.Reader(new Configuration(true), fileOption);
            Validate.isTrue(this.sequenceFileReader.getKeyClass().equals(NullWritable.class));
            Validate.isTrue(this.sequenceFileReader.getValueClass().equals(BytesWritable.class));
            this.tryAdvance();
        }

        private void tryAdvance() {
            if (this.nextElements != null && ++this.nextIndex < this.nextElements.length) return;
            try {
                if (!this.sequenceFileReader.next(this.nullWritable, this.bytesWritable)) {
                    this.nextElements = null;
                    return;
                }
                this.nextElements = (Object[]) new ObjectInputStream(new ByteArrayInputStream(this.bytesWritable.getBytes())).readObject();
                this.nextIndex = 0;
            } catch (IOException | ClassNotFoundException e) {
                this.nextElements = null;
                IOUtils.closeQuietly(this);
                throw new RheemException("Reading failed.", e);
            }
        }

        @Override
        public boolean hasNext() {
            return this.nextElements != null;
        }

        @Override
        public T next() {
            Validate.isTrue(this.hasNext());
            final T result = (T) this.nextElements[this.nextIndex];
            this.tryAdvance();
            return result;
        }

        @Override
        public void close() {
            if (this.sequenceFileReader != null) {
                try {
                    this.sequenceFileReader.close();
                } catch (Throwable t) {
                    LoggerFactory.getLogger(this.getClass()).error("Closing failed.", t);
                }
                this.sequenceFileReader = null;
            }
        }
    }
}
