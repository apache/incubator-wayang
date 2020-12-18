package io.rheem.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import io.rheem.rheem.basic.channels.FileChannel;
import io.rheem.rheem.basic.data.Tuple2;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.plan.rheemplan.Operator;
import io.rheem.rheem.core.plan.rheemplan.UnarySource;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.util.Tuple;
import io.rheem.rheem.core.util.fs.FileSystems;
import io.rheem.rheem.spark.channels.RddChannel;
import io.rheem.rheem.spark.execution.SparkExecutor;
import io.rheem.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see SparkObjectFileSink
 */
public class SparkTsvFileSource<T> extends UnarySource<T> implements SparkExecutionOperator {

    private final String sourcePath;

    public SparkTsvFileSource(DataSetType type) {
        this(null, type);
    }

    public SparkTsvFileSource(String sourcePath, DataSetType<T> type) {
        super(type);
        this.sourcePath = sourcePath;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        final String sourcePath;
        if (this.sourcePath != null) {
            assert inputs.length == 0;
            sourcePath = this.sourcePath;
        } else {
            FileChannel.Instance input = (FileChannel.Instance) inputs[0];
            sourcePath = input.getSinglePath();
        }
        RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final String actualInputPath = FileSystems.findActualSingleInputPath(sourcePath);
        final JavaRDD<String> linesRdd = sparkExecutor.sc.textFile(actualInputPath);
        this.name(linesRdd);
        final JavaRDD<T> dataQuantaRdd = linesRdd
                .map(line -> {
                    // TODO: Important. Enrich type informations to create the correct parser!
                    int tabPos = line.indexOf('\t');
                    return (T) new Tuple2<>(
                            Integer.valueOf(line.substring(0, tabPos)),
                            Float.valueOf(line.substring(tabPos + 1)));
                });
        this.name(dataQuantaRdd);

        output.accept(dataQuantaRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }


    @Override
    protected ExecutionOperator createCopy() {
        return new SparkTsvFileSource<>(this.sourcePath, this.getType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.spark.tsvfilesource.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_TSV_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

}
