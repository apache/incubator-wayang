package org.qcri.rheem.flink.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.execution.FlinkExecutor;
import org.qcri.rheem.flink.platform.FlinkPlatform;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


/**
 * {@link Operator} for the {@link FlinkPlatform} that creates a sequence file.
 *
 * @see FlinkObjectFileSource
 */
public class FlinkObjectFileSource<Type> extends UnarySource<Type> implements FlinkExecutionOperator {

    private final String sourcePath;

    public FlinkObjectFileSource(DataSetType<Type> type) {
        this(null, type);
    }

    public FlinkObjectFileSource(String sourcePath, DataSetType<Type> type) {
        super(type);
        this.sourcePath = sourcePath;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext
    ) throws Exception {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final String path;
        if (this.sourcePath == null) {
            final FileChannel.Instance input = (FileChannel.Instance) inputs[0];
            path = input.getSinglePath();
        } else {
            assert inputs.length == 0;
            path = this.sourcePath;
        }
        DataSetChannel.Instance output = (DataSetChannel.Instance) outputs[0];
        flinkExecutor.fee.setParallelism(flinkExecutor.getNumDefaultPartitions());

        HadoopInputFormat<NullWritable, BytesWritable> _file = HadoopInputs.readSequenceFile(NullWritable.class, BytesWritable.class, path);
        final DataSet<Tuple2> dataSet =
                flinkExecutor
                        .fee.createInput(_file)
                        .setParallelism(flinkExecutor.getNumDefaultPartitions())
                        .flatMap(new FlatMapFunction<org.apache.flink.api.java.tuple.Tuple2<NullWritable,BytesWritable>, Tuple2>() {
                            @Override
                            public void flatMap(org.apache.flink.api.java.tuple.Tuple2<NullWritable, BytesWritable> value, Collector<Tuple2> out) throws Exception {
                                Object tmp = new ObjectInputStream(new ByteArrayInputStream(value.f1.getBytes())).readObject();
                                for(Object element: (Iterable)tmp){
                                    out.collect((Tuple2) element);
                                }
                            }

                        });

        output.accept(dataSet, flinkExecutor);

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkObjectFileSource<Type>(sourcePath, this.getType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.objectfilesource.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Collections.singletonList(FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return true;
    }
}
