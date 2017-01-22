package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.data.Tuple2;
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
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * {@link Operator} for the {@link JavaPlatform} that creates a TSV file.
 * Only applicable to tuples with standard datatypes.
 *
 * @see JavaObjectFileSource
 */
public class JavaTsvFileSink<T extends Tuple2<?, ?>> extends UnarySink<T> implements JavaExecutionOperator {

    private final String targetPath;

    public JavaTsvFileSink(DataSetType<T> type) {
        this(null, type);
    }

    public JavaTsvFileSink(String targetPath, DataSetType<T> type) {
        super(type);
        assert type.equals(DataSetType.createDefault(Tuple2.class)) :
                String.format("Illegal type for %s: %s", this, type);
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
        final FileSystem fileSystem = FileSystems.getFileSystem(path).orElseThrow(
                () -> new IllegalStateException(String.format("No file system found for \"%s\".", this.targetPath))
        );

        try (final BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(
                        fileSystem.create(path), "UTF-8"
                )
        )) {
            try {
                ((JavaChannelInstance) inputs[0]).provideStream().forEach(
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

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.tsvfilesink.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaTsvFileSink<>(this.targetPath, this.getType());
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
