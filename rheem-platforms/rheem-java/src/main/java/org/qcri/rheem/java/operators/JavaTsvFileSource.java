package org.qcri.rheem.java.operators;

import org.apache.commons.io.IOUtils;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.function.Function;
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

        final String actualInputPath = FileSystems.findActualSingleInputPath(this.sourcePath);
        Stream<T> stream = this.createStream(actualInputPath);
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

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaTsvFileSource<>(this.sourcePath, this.getType());
    }

}
