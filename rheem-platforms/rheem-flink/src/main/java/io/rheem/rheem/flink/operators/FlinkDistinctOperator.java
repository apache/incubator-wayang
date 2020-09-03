package io.rheem.rheem.flink.operators;

import org.apache.flink.api.java.DataSet;
import io.rheem.rheem.basic.operators.DistinctOperator;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.util.Tuple;
import io.rheem.rheem.flink.channels.DataSetChannel;
import io.rheem.rheem.flink.compiler.KeySelectorDistinct;
import io.rheem.rheem.flink.execution.FlinkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


/**
 * Flink implementation of the {@link DistinctOperator}.
 */
public class FlinkDistinctOperator<Type>
        extends DistinctOperator<Type>
        implements FlinkExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public FlinkDistinctOperator(DataSetType<Type> type) {
        super(type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkDistinctOperator(DistinctOperator<Type> that) {
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

        final DataSetChannel.Instance input = (DataSetChannel.Instance) inputs[0];
        final DataSetChannel.Instance output = (DataSetChannel.Instance) outputs[0];

        final DataSet<Type> dataSetInput = input.provideDataSet();

        final DataSet<Type> dataSetOutput = dataSetInput.distinct(new KeySelectorDistinct<Type>());

        output.accept(dataSetOutput, flinkExecutor);


        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkDistinctOperator<>(this);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.distinct.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }


}
