package org.qcri.rheem.flink.operators;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.qcri.rheem.basic.operators.GroupByOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.execution.FlinkExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Flink implementation of the {@link GroupByOperator}.
 */
public class FlinkGroupByOperator<InputType, KeyType> extends GroupByOperator<InputType, KeyType> implements FlinkExecutionOperator {

    /**
     * Creates a new instance.
     */
    public FlinkGroupByOperator(TransformationDescriptor<InputType, KeyType> keydescriptor,
                                DataSetType<InputType> inputType,
                                DataSetType<KeyType> keyType ) {
        super((FunctionDescriptor.SerializableFunction<InputType, KeyType>) keydescriptor.getJavaImplementation(),
                inputType.getDataUnitType().getTypeClass(),
                keyType.getDataUnitType().getTypeClass());
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkGroupByOperator(GroupByOperator<InputType, KeyType> that) {
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
        final DataSetChannel.Instance input  = (DataSetChannel.Instance) inputs[0];
        final DataSetChannel.Instance output = (DataSetChannel.Instance) outputs[0];


        final KeySelector<InputType, KeyType> keySelector = flinkExecutor.compiler.compileKeySelector(this.getKeyDescriptor());

        final DataSet<InputType> dataSetInput  = input.provideDataSet();


        final DataSet<Iterable<InputType>> dataSetOutput = dataSetInput.groupBy(keySelector).reduceGroup(
                (GroupReduceFunction<InputType, Iterable<InputType>>) (iterable, collector) -> {
                    Collection<InputType> dataUnitGroup = new ArrayList<>();
                    iterable.forEach(dataUnitGroup::add);
                    collector.collect(dataUnitGroup);
                }
        );

        output.accept(dataSetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkGroupByOperator<InputType, KeyType>(this);
    }


    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.groupby.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                FlinkExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.getKeyDescriptor(), configuration);
        return optEstimator;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }
}
