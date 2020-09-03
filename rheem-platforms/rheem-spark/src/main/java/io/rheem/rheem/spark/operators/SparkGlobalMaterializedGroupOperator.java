package io.rheem.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import io.rheem.rheem.basic.operators.GlobalMaterializedGroupOperator;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.util.Tuple;
import io.rheem.rheem.spark.channels.BroadcastChannel;
import io.rheem.rheem.spark.channels.RddChannel;
import io.rheem.rheem.spark.execution.SparkExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Spark implementation of the {@link GlobalMaterializedGroupOperator}.
 */
public class SparkGlobalMaterializedGroupOperator<Type>
        extends GlobalMaterializedGroupOperator<Type>
        implements SparkExecutionOperator {


    public SparkGlobalMaterializedGroupOperator(DataSetType<Type> inputType, DataSetType<Iterable<Type>> outputType) {
        super(inputType, outputType);
    }

    public SparkGlobalMaterializedGroupOperator(Class<Type> typeClass) {
        super(typeClass);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkGlobalMaterializedGroupOperator(GlobalMaterializedGroupOperator<Type> that) {
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

        final RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final JavaRDD<Type> inputRdd = input.provideRdd();
        final JavaRDD<Type> coalescedRdd = inputRdd.coalesce(1);
        this.name(coalescedRdd);
        final JavaRDD<Iterable<Type>> outputRdd = coalescedRdd
                .mapPartitions(partitionIterator -> {
                    Collection<Type> dataUnitGroup = new ArrayList<>();
                    while (partitionIterator.hasNext()) {
                        dataUnitGroup.add(partitionIterator.next());
                    }
                    return (Iterator)Collections.singleton(dataUnitGroup).iterator();
                });
        this.name(outputRdd);

        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkGlobalMaterializedGroupOperator<>(this.getInputType(), this.getOutputType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.spark.globalgroup.load";
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
