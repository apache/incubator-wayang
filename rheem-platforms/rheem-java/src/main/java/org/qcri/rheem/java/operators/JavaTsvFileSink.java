package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * {@link Operator} for the {@link JavaPlatform} that creates a TSV file.
 * Only applicable to tuples with standard datatypes.
 *
 * @see JavaObjectFileSource
 */
public class JavaTsvFileSink<T extends Tuple2<?, ?>> extends UnarySink<T> implements JavaExecutionOperator {

    private FileChannel.Instance outputChannelExecutor;

    private final String targetPath;

    public JavaTsvFileSink(String targetPath, DataSetType type) {
        super(type, null);
        assert type.equals(DataSetType.createDefault(Tuple2.class)) :
        String.format("Illegal type for %s: %s", this, type);
        this.targetPath = targetPath;
    }

    @Override
    public void evaluate(JavaChannelInstance[] inputs, JavaChannelInstance[] outputs, FunctionCompiler compiler) {
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
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(Configuration configuration) {
        // NB: Not actually measured.
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(1, 0, .8d, (inputCards, outputCards) -> 1000 * inputCards[0] + 810000),
                new DefaultLoadEstimator(1, 0, 0, (inputCards, outputCards) -> 0),
                new DefaultLoadEstimator(1, 0, 0, (inputCards, outputCards) -> inputCards[0] * 256),
                new DefaultLoadEstimator(1, 0, 0, (inputCards, outputCards) -> 0)
        );
        return Optional.of(mainEstimator);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaTsvFileSink<>(this.targetPath, this.getType());
    }

    @Override
    public void instrumentSink(JavaChannelInstance channelExecutor) {
        this.outputChannelExecutor = (FileChannel.Instance) channelExecutor; // TODO: What is this doing????
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
        return Collections.singletonList(FileChannel.HDFS_TSV_DESCRIPTOR);
    }

}
