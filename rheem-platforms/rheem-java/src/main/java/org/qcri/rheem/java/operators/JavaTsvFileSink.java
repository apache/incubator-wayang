package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.channels.HdfsFileInitializer;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;

/**
 * {@link Operator} for the {@link JavaPlatform} that creates a TSV file.
 * Only applicable to tuples with standard datatypes.
 *
 * @see JavaObjectFileSource
 */
public class JavaTsvFileSink<T extends Tuple2<?, ?>> extends UnarySink<T> implements JavaExecutionOperator {

    private HdfsFileInitializer.Executor outputChannelExecutor;

    private final String targetPath;

    public JavaTsvFileSink(String targetPath, DataSetType type) {
        super(type, null);
        assert type.equals(DataSetType.createDefault(Tuple2.class)) :
        String.format("Illegal type for %s: %s", this, type);
        this.targetPath = targetPath;
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();

        // Prepare Hadoop's SequenceFile.Writer.
        final FileSystem fileSystem = FileSystems.getFileSystem(this.targetPath).orElseThrow(
                () -> new IllegalStateException(String.format("No file system found for \"%s\".", this.targetPath))
        );

        try (final BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(
                        fileSystem.create(this.targetPath), "UTF-8"
                )
        )) {
            try {
                inputs[0].provideStream().forEach(
                        dataQuantum -> {
                            try {
                                // TODO: Once there are more tuple types, make this generic.
                                @SuppressWarnings("unchecked")
                                Tuple2<Object, Object> tuple2 = (Tuple2<Object, Object>) dataQuantum;
                                writer.append(String.valueOf(tuple2.field0))
                                        .append('\t')
                                        .append(String.valueOf(tuple2.field1))
                                        .append('\n');
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }
                );
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        } catch (IOException e) {
            throw new RheemException(String.format("%s failed on writing to %s.", this, this.targetPath), e);
        }
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaTsvFileSink<>(this.targetPath, this.getType());
    }

    @Override
    public void instrumentSink(ChannelExecutor channelExecutor) {
        this.outputChannelExecutor = (HdfsFileInitializer.Executor) channelExecutor; // TODO: What is this doing????
    }

    public String getTargetPath() {
        return this.targetPath;
    }

}
