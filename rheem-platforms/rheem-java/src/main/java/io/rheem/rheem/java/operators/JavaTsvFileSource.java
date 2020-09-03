package io.rheem.rheem.java.operators;

import org.apache.commons.io.IOUtils;
import io.rheem.rheem.basic.channels.FileChannel;
import io.rheem.rheem.basic.data.Record;
import io.rheem.rheem.basic.data.Tuple2;
import io.rheem.rheem.core.api.exception.RheemException;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.plan.rheemplan.Operator;
import io.rheem.rheem.core.plan.rheemplan.UnarySource;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.util.Tuple;
import io.rheem.rheem.core.util.fs.FileSystem;
import io.rheem.rheem.core.util.fs.FileSystems;
import io.rheem.rheem.core.util.fs.FileUtils;
import io.rheem.rheem.java.channels.StreamChannel;
import io.rheem.rheem.java.execution.JavaExecutor;
import io.rheem.rheem.java.platform.JavaPlatform;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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
        super(type);
        this.sourcePath = sourcePath;
    }

    public JavaTsvFileSource(DataSetType<Tuple2> type) {
        this(null, type);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert outputs.length == this.getNumOutputs();

        final String path;
        if (this.sourcePath == null) {
            final FileChannel.Instance input = (FileChannel.Instance) inputs[0];
            path = input.getSinglePath();
        } else {
            assert inputs.length == 0;
            path = this.sourcePath;
        }
        final String actualInputPath = FileSystems.findActualSingleInputPath(path);
        Stream<T> stream = this.createStream(actualInputPath);
        ((StreamChannel.Instance) outputs[0]).accept(stream);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    private Stream<T> createStream(String path) {
        Function<String, T> parser = this::lineParse;

        return FileUtils.streamLines(path).map(parser);
    }

    private T lineParse(String line) {
        // TODO rewrite in less verbose way.
        Class typeClass = this.getType().getDataUnitType().getTypeClass();
        int tabPos = line.indexOf('\t');
        if (tabPos == -1) {
            if (typeClass == Integer.class) {
                return (T) Integer.valueOf(line);
            } else if (typeClass == Float.class) {
                return (T) Float.valueOf(line);
            } else if (typeClass == String.class) {
                return (T) String.valueOf(line);
            } else throw new RheemException(String.format("Cannot parse TSV file line %s", line));
        } else if (typeClass == Record.class) {
            // TODO: Fix Record parsing.
            return (T) new Record();
        } else if (typeClass == Tuple2.class) {
            // TODO: Fix Tuple2 parsing
            return (T) new Tuple2(
                    Integer.valueOf(line.substring(0, tabPos)),
                    Float.valueOf(line.substring(tabPos + 1)));
        } else
            throw new RheemException(String.format("Cannot parse TSV file line %s", line));

    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.tsvfilesource.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaTsvFileSource<>(this.sourcePath, this.getType());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_TSV_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
