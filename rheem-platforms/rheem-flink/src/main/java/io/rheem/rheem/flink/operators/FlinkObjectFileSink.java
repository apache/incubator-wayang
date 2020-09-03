package io.rheem.rheem.flink.operators;

import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.core.fs.FileSystem;
import io.rheem.rheem.basic.channels.FileChannel;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.plan.rheemplan.Operator;
import io.rheem.rheem.core.plan.rheemplan.UnarySink;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.util.Tuple;
import io.rheem.rheem.flink.channels.DataSetChannel;
import io.rheem.rheem.flink.compiler.RheemFileOutputFormat;
import io.rheem.rheem.flink.execution.FlinkExecutor;
import io.rheem.rheem.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * {@link Operator} for the {@link FlinkPlatform} that creates a sequence file.
 *
 * @see FlinkObjectFileSink
 */
public class FlinkObjectFileSink<Type> extends UnarySink<Type> implements FlinkExecutionOperator {

    private final String targetPath;


    public FlinkObjectFileSink(DataSetType<Type> type) {
        this(null, type);
    }

    public FlinkObjectFileSink(String targetPath, DataSetType<Type> type) {
        super(type);
        this.targetPath = targetPath;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext
    ) throws Exception {

        assert inputs.length == this.getNumInputs();
        assert outputs.length <= 1;

        final FileChannel.Instance output = (FileChannel.Instance) outputs[0];
        final String targetPath = output.addGivenOrTempPath(this.targetPath, flinkExecutor.getConfiguration());

        DataSetChannel.Instance input = (DataSetChannel.Instance) inputs[0];
        final DataSink<Type> tDataSink = input.<Type>provideDataSet()
                .write(new RheemFileOutputFormat<Type>(targetPath), targetPath, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);


        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkObjectFileSink<>(targetPath, this.getType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.objectfilesink.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return true;
    }
}
