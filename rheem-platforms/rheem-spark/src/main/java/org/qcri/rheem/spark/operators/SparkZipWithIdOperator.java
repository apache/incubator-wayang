package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.ZipWithIdOperator;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.spark.channels.BroadcastChannel;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


/**
 * Spark implementation of the {@link MapOperator}.
 */
public class SparkZipWithIdOperator<InputType>
        extends ZipWithIdOperator<InputType>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     */
    public SparkZipWithIdOperator(DataSetType<InputType> inputType) {
        super(inputType);
    }

    /**
     * Creates a new instance.
     */
    public SparkZipWithIdOperator(Class<InputType> inputTypeClass) {
        super(inputTypeClass);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkZipWithIdOperator(ZipWithIdOperator<InputType> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final JavaRDD<InputType> inputRdd = input.provideRdd();
        final JavaPairRDD<InputType, Long> zippedRdd = inputRdd.zipWithUniqueId();
        this.name(zippedRdd);
        final JavaRDD<Tuple2<Long, InputType>> outputRdd = zippedRdd.map(pair -> new Tuple2<>(pair._2, pair._1));
        this.name(outputRdd);

        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkZipWithIdOperator<>(this.getInputType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.spark.zipwithid.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        if (index == 0) {
            return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
        } else {
            return Collections.singletonList(BroadcastChannel.DESCRIPTOR);
        }
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
