package io.rheem.rheem.flink.operators;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import io.rheem.rheem.basic.operators.SortOperator;
import io.rheem.rheem.core.function.TransformationDescriptor;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.util.Tuple;
import io.rheem.rheem.flink.channels.DataSetChannel;
import io.rheem.rheem.flink.compiler.FunctionCompiler;
import io.rheem.rheem.flink.execution.FlinkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Flink implementation of the {@link SortOperator}.
 */
public class FlinkSortOperator<InputType, KeyType>
        extends SortOperator<InputType, KeyType>
        implements FlinkExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public FlinkSortOperator(TransformationDescriptor<InputType, KeyType> keyDescriptor, DataSetType<InputType> type) {
        super(keyDescriptor, type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkSortOperator(SortOperator<InputType, KeyType> that) {
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

        final DataSet<InputType> dataSetInput = input.provideDataSet();

        FunctionCompiler compiler = flinkExecutor.getCompiler();

        final KeySelector<InputType, KeyType> keyExtractor = compiler.compileKeySelector(this.keyDescriptor);

        final DataSet<InputType> dataSetOutput = dataSetInput.sortPartition(keyExtractor, Order.ASCENDING);

        output.accept(dataSetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkSortOperator<>(this.getKeyDescriptor(), this.getInputType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.sort.load";
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

    @Override
    public boolean containsAction() {
        return false;
    }
}
