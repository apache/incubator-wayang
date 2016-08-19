package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.BroadcastChannel;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.util.*;


/**
 * Spark implementation of the {@link SparkMapPartitionsOperator}.
 */
public class SparkMapPartitionsOperator<InputType, OutputType>
        extends MapOperator<InputType, OutputType>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     */
    public SparkMapPartitionsOperator(TransformationDescriptor<InputType, OutputType> functionDescriptor,
                                      DataSetType<InputType> inputType, DataSetType<OutputType> outputType) {
        super(functionDescriptor, inputType, outputType);
    }

    /**
     * Creates a new instance.
     */
    public SparkMapPartitionsOperator(TransformationDescriptor<InputType, OutputType> functionDescriptor) {
        this(functionDescriptor,
                DataSetType.createDefault(functionDescriptor.getInputType()),
                DataSetType.createDefault(functionDescriptor.getOutputType()));
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkMapPartitionsOperator(MapOperator<InputType, OutputType> that) {
        super(that);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs,
                         ChannelInstance[] outputs,
                         SparkExecutor sparkExecutor,
                         OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final FlatMapFunction<Iterator<InputType>, OutputType> mapFunction =
                sparkExecutor.getCompiler().compileForMapPartitions(this.functionDescriptor, this, operatorContext, inputs);

        final JavaRDD<InputType> inputRdd = input.provideRdd();
        final JavaRDD<OutputType> outputRdd = inputRdd.mapPartitions(mapFunction);
        this.name(outputRdd);
        output.accept(outputRdd, sparkExecutor);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkMapPartitionsOperator<>(this.getFunctionDescriptor(), this.getInputType(), this.getOutputType());
    }

    @Override
    public Optional<LoadProfileEstimator<ExecutionOperator>> createLoadProfileEstimator(Configuration configuration) {
        final NestableLoadProfileEstimator<ExecutionOperator> mainEstimator =
                LoadProfileEstimators.createFromJuelSpecification(
                        configuration.getStringProperty("rheem.spark.mappartitions.load")
                );
        final LoadProfileEstimator udfEstimator = configuration
                .getFunctionLoadProfileEstimatorProvider()
                .provideFor(this.functionDescriptor);
        mainEstimator.nest(udfEstimator);
        return Optional.of(mainEstimator);
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
    public boolean isExecutedEagerly() {
        return false;
    }
}

