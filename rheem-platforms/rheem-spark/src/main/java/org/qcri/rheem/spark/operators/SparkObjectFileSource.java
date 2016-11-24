package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a sequence file.
 *
 * @see SparkObjectFileSink
 */
public class SparkObjectFileSource<T> extends UnarySource<T> implements SparkExecutionOperator {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final String sourcePath;

    public SparkObjectFileSource(DataSetType<T> type) {
        this(null, type);
    }

    public SparkObjectFileSource(String sourcePath, DataSetType<T> type) {
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
        final JavaRDD<Object> rdd = sparkExecutor.sc.objectFile(actualInputPath);
        this.name(rdd);
        output.accept(rdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkObjectFileSource<>(this.sourcePath, this.getType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.spark.objectfilesource.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR);
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
