package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a TSV file.
 * Only applicable to tuples with standard datatypes.
 *
 * @see SparkObjectFileSource
 */
public class SparkTsvFileSink<T extends Tuple2<?, ?>> extends UnarySink<T> implements SparkExecutionOperator {

    private final String targetPath;

    public SparkTsvFileSink(DataSetType<T> type) {
        this(null, type);
    }

    public SparkTsvFileSink(String targetPath, DataSetType<T> type) {
        super(type);
        assert type.equals(DataSetType.createDefault(Tuple2.class)) :
                String.format("Illegal type for %s: %s", this, type);
        this.targetPath = targetPath;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();

        final FileChannel.Instance output = (FileChannel.Instance) outputs[0];
        final String targetPath = output.addGivenOrTempPath(this.targetPath, sparkExecutor.getConfiguration());

        final RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        final JavaRDD<Object> rdd = input.provideRdd();
        final JavaRDD<String> serializedRdd = rdd
                .map(dataQuantum -> {
                    // TODO: Once there are more tuple types, make this generic.
                    @SuppressWarnings("unchecked")
                    Tuple2<Object, Object> tuple2 = (Tuple2<Object, Object>) dataQuantum;
                    return String.valueOf(tuple2.field0) + '\t' + String.valueOf(tuple2.field1);
                });
        this.name(serializedRdd);
        serializedRdd
                .coalesce(1) // TODO: Allow more than one TSV file?
                .saveAsTextFile(targetPath);


        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkTsvFileSink<>(this.targetPath, this.getType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.spark.tsvfilesink.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
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
