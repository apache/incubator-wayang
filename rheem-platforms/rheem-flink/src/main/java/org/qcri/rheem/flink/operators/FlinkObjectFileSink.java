package org.qcri.rheem.flink.operators;

import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.core.fs.FileSystem;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.compiler.RheemFileOutputFormat;
import org.qcri.rheem.flink.execution.FlinkExecutor;
import org.qcri.rheem.flink.platform.FlinkPlatform;

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
