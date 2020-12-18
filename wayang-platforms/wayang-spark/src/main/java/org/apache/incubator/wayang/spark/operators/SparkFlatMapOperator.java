package org.apache.incubator.wayang.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.incubator.wayang.basic.operators.FlatMapOperator;
import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.function.FlatMapDescriptor;
import org.apache.incubator.wayang.core.optimizer.OptimizationContext;
import org.apache.incubator.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.incubator.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.incubator.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.incubator.wayang.core.platform.ChannelDescriptor;
import org.apache.incubator.wayang.core.platform.ChannelInstance;
import org.apache.incubator.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.incubator.wayang.core.types.DataSetType;
import org.apache.incubator.wayang.core.util.Tuple;
import org.apache.incubator.wayang.spark.channels.BroadcastChannel;
import org.apache.incubator.wayang.spark.channels.RddChannel;
import org.apache.incubator.wayang.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;


/**
 * Spark implementation of the {@link FlatMapOperator}.
 */
public class SparkFlatMapOperator<InputType, OutputType>
        extends FlatMapOperator<InputType, OutputType>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param functionDescriptor
     */
    public SparkFlatMapOperator(DataSetType inputType, DataSetType outputType,
                                FlatMapDescriptor<InputType, OutputType> functionDescriptor) {
        super(functionDescriptor, inputType, outputType);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkFlatMapOperator(FlatMapOperator<InputType, OutputType> that) {
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

        final FlatMapFunction<InputType, OutputType> flatMapFunction =
                sparkExecutor.getCompiler().compile(this.functionDescriptor, this, operatorContext, inputs);

        final JavaRDD<InputType> inputRdd = input.provideRdd();
        final JavaRDD<OutputType> outputRdd = inputRdd.flatMap(flatMapFunction);
        this.name(outputRdd);
        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkFlatMapOperator<>(this.getInputType(), this.getOutputType(), this.getFunctionDescriptor());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.flatmap.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                SparkExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.functionDescriptor, configuration);
        return optEstimator;
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
