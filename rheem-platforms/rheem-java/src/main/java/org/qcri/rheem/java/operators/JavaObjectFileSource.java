package org.qcri.rheem.java.operators;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.qcri.rheem.core.api.exception.RheemException;
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
            final String path = this.findCorrectInputPath(this.sourcePath);
            sequenceFileIterator = new SequenceFileIterator<>(path);
            Stream<?> sequenceFileStream =
                    StreamSupport.stream(Spliterators.spliteratorUnknownSize(sequenceFileIterator, 0), false);
            outputs[0].acceptStream(sequenceFileStream);
        } catch (IOException e) {
            throw new RheemException("Reading failed.", e);
        }
    }

    /**
     * Systems such as Spark do not produce a single output file often times. That method tries to detect such
     * split object files to reassemble them correctly.
     */
    private String findCorrectInputPath(String ostensibleInputFile) {
        final Optional<FileSystem> fileSystem = FileSystems.getFileSystem(ostensibleInputFile);

        if (!fileSystem.isPresent()) {
            LoggerFactory.getLogger(this.getClass()).warn("Could not inspect input file {}.", this.sourcePath);
            return this.sourcePath;

        } else if (fileSystem.get().isDirectory(this.sourcePath)) {
            final Collection<String> children = fileSystem.get().listChildren(this.sourcePath);

            // Look for Spark-like directory structure.
            if (children.stream().anyMatch(child -> child.endsWith("_SUCCESS"))) {
                final List<String> sparkFiles =
                        children.stream().filter(child -> child.matches(".*/part-\\d{5}")).collect(Collectors.toList());
                if (sparkFiles.size() != 1) {
                    throw new RheemException("Illegal number of Spark result files: " + sparkFiles.size());
                }
                LoggerFactory.getLogger(this.getClass()).info("Using input path {} for {}.", sparkFiles.get(0), this);
                return sparkFiles.get(0);
            } else {
                throw new RheemException("Could not identify directory structure: " + children);
            }

        } else {
            return this.sourcePath;
        }
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
