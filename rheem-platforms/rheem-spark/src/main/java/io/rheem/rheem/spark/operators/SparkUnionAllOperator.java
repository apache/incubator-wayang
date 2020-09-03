package io.rheem.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import io.rheem.rheem.basic.operators.UnionAllOperator;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.util.Tuple;
import io.rheem.rheem.spark.channels.RddChannel;
import io.rheem.rheem.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Spark implementation of the {@link UnionAllOperator}.
 */
public class SparkUnionAllOperator<Type>
        extends UnionAllOperator<Type>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     */
    public SparkUnionAllOperator(DataSetType<Type> type) {
        super(type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkUnionAllOperator(UnionAllOperator<Type> that) {
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

        RddChannel.Instance input0 = (RddChannel.Instance) inputs[0];
        RddChannel.Instance input1 = (RddChannel.Instance) inputs[1];
        RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final JavaRDD<Type> inputRdd0 = input0.provideRdd();
        final JavaRDD<Type> inputRdd1 = input1.provideRdd();
        final JavaRDD<Type> outputRdd = inputRdd0.union(inputRdd1);
        this.name(outputRdd);

        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkUnionAllOperator<>(this.getInputType0());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.spark.union.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
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
