package org.apache.incubator.wayang.flink.operators;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.incubator.wayang.basic.operators.MaterializedGroupByOperator;
import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.function.TransformationDescriptor;
import org.apache.incubator.wayang.core.optimizer.OptimizationContext;
import org.apache.incubator.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.incubator.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.incubator.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.incubator.wayang.core.platform.ChannelDescriptor;
import org.apache.incubator.wayang.core.platform.ChannelInstance;
import org.apache.incubator.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.incubator.wayang.core.types.DataSetType;
import org.apache.incubator.wayang.core.util.Tuple;
import org.apache.incubator.wayang.flink.channels.DataSetChannel;
import org.apache.incubator.wayang.flink.execution.FlinkExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Flink implementation of the {@link MaterializedGroupByOperator}.
 */
public class FlinkMaterializedGroupByOperator<Type, KeyType>
        extends MaterializedGroupByOperator<Type, KeyType>
        implements FlinkExecutionOperator {

    public FlinkMaterializedGroupByOperator(TransformationDescriptor<Type, KeyType> keyDescriptor,
                                            DataSetType<Type> inputType,
                                            DataSetType<Iterable<Type>> outputType) {
        super(keyDescriptor, inputType, outputType);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkMaterializedGroupByOperator(MaterializedGroupByOperator<Type, KeyType> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        DataSetChannel.Instance input = (DataSetChannel.Instance) inputs[0];
        DataSetChannel.Instance output = (DataSetChannel.Instance) outputs[0];

        final DataSet<Type> dataSetInput = input.provideDataSet();
        final KeySelector<Type, KeyType> keyExtractor =
                flinkExecutor.getCompiler().compileKeySelector(this.getKeyDescriptor());


        final DataSet<Iterable<Type>> dataSetOutput =
                dataSetInput
                    .groupBy(keyExtractor)
                    .reduceGroup(
                        (GroupReduceFunction<Type, Iterable<Type>>) (iterable, collector) -> {
                            Collection<Type> dataUnitGroup = new ArrayList<>();
                            iterable.forEach(dataUnitGroup::add);
                            collector.collect(dataUnitGroup);
                        }
                    );

        output.accept(dataSetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkMaterializedGroupByOperator<>(this.getKeyDescriptor(), this.getInputType(), this.getOutputType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.flink.groupby.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                FlinkExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor, configuration);
        return optEstimator;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public boolean containsAction() {
        return false;
    }
}
