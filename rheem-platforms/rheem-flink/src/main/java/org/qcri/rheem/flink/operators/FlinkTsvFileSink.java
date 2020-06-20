package org.qcri.rheem.flink.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.execution.FlinkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by bertty on 31-10-17.
 */
public class FlinkTsvFileSink<Type extends Tuple2<?, ?>> extends UnarySink<Type> implements FlinkExecutionOperator {

    private final String targetPath;

    public FlinkTsvFileSink(DataSetType<Type> type) {
        this(null, type);
    }

    public FlinkTsvFileSink(String targetPath, DataSetType<Type> type) {
        super(type);
        assert type.equals(DataSetType.createDefault(Tuple2.class)) :
                String.format("Illegal type for %s: %s", this, type);
        this.targetPath = targetPath;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();

        final FileChannel.Instance output = (FileChannel.Instance) outputs[0];
        final String targetPath = output.addGivenOrTempPath(this.targetPath, flinkExecutor.getConfiguration());

        final DataSetChannel.Instance input = (DataSetChannel.Instance) inputs[0];

        DataSet<String> map = input.<Type>provideDataSet()
                .map(new MapFunction<Type, String>() {
                    private Type dataQuantum;

                    @Override
                    public String map(Type dataQuantum) throws Exception {
                        this.dataQuantum = dataQuantum;
                        Tuple2 tuple2 = (Tuple2) dataQuantum;
                        return String.valueOf(tuple2.field0) + '\t' + String.valueOf(tuple2.field1);                    }
                }).setParallelism(flinkExecutor.getNumDefaultPartitions());

        map.writeAsText(targetPath).setParallelism(1);


        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkTsvFileSink<>(this.targetPath, this.getType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.tsvfilesink.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(DataSetChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_TSV_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return true;
    }

}
