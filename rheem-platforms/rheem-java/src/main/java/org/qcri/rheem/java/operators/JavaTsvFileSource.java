package org.qcri.rheem.java.operators;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link Operator} for the {@link JavaPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see JavaObjectFileSink
 */
public class JavaTsvFileSource<T> extends UnarySource<T> implements JavaExecutionOperator {

    private final String sourcePath;

    public JavaTsvFileSource(String sourcePath, DataSetType type) {
        super(type, null);
        this.sourcePath = sourcePath;
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert outputs.length == this.getNumOutputs();

        final String path = this.findCorrectInputPath(this.sourcePath);
        Stream<T> stream = this.createStream(path);
        outputs[0].acceptStream(stream);
    }

    private Stream<T> createStream(String path) {
        // TODO: Important. Enrich type informations to create the correct parser!
        Function<String, T> parser = (line) -> {
            int tabPos = line.indexOf('\t');
            return (T) new Tuple2<>(
                    Integer.valueOf(line.substring(0, tabPos)),
                    Float.valueOf(line.substring(tabPos + 1)));
        };

        return this.streamLines(path).map(parser);
    }

    /**
     * Creates a {@link Stream} of a lines of the file.
     *
     * @param path of the file
     * @return the {@link Stream}
     */
    private Stream<String> streamLines(String path) {
        final FileSystem fileSystem = FileSystems.getFileSystem(path).orElseThrow(
                () -> new IllegalStateException(String.format("No file system found for %s", path))
        );
        try {
            Iterator<String> lineIterator = this.createLineIterator(fileSystem, path);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(lineIterator, 0), false);
        } catch (IOException e) {
            throw new RheemException(String.format("%s failed to read %s.", this, path), e);
        }

    }

    /**
     * Creates an {@link Iterator} over the lines of a given {@code path} (that resides in the given {@code fileSystem}).
     */
    private Iterator<String> createLineIterator(FileSystem fileSystem, String path) throws IOException {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(path), "UTF-8"));
        return new Iterator<String>() {

            String next;

            {
                this.advance();
            }

            private void advance() {
                try {
                    this.next = reader.readLine();
                } catch (IOException e) {
                    this.next = null;
                    throw new UncheckedIOException(e);
                } finally {
                    if (this.next == null) {
                        IOUtils.closeQuietly(reader);
                    }
                }
            }

            @Override
            public boolean hasNext() {
                return this.next != null;
            }

            @Override
            public String next() {
                assert this.hasNext();
                final String returnValue = this.next;
                this.advance();
                return returnValue;
            }
        };
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
                    throw new RheemException("Illegal number of Spark result files: " + sparkFiles);
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
        return new JavaTsvFileSource<>(this.sourcePath, this.getType());
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
