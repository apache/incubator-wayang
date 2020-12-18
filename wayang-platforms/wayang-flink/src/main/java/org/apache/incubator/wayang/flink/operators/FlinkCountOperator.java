package org.apache.incubator.wayang.flink.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.incubator.wayang.basic.operators.CountOperator;
import org.apache.incubator.wayang.core.api.exception.WayangException;
import org.apache.incubator.wayang.core.optimizer.OptimizationContext;
import org.apache.incubator.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.incubator.wayang.core.platform.ChannelDescriptor;
import org.apache.incubator.wayang.core.platform.ChannelInstance;
import org.apache.incubator.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.incubator.wayang.core.types.DataSetType;
import org.apache.incubator.wayang.core.util.Tuple;
import org.apache.incubator.wayang.flink.channels.DataSetChannel;
import org.apache.incubator.wayang.flink.execution.FlinkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Flink implementation of the {@link CountOperator}.
 */
public class FlinkCountOperator<Type> extends CountOperator<Type>
        implements FlinkExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public FlinkCountOperator(DataSetType<Type> type) {
        super(type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkCountOperator(CountOperator<Type> that) {
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

        final Long count;
        try {

            count = input.provideDataSet().count();
        } catch (Exception e) {
            throw new WayangException(e);
        }
        final DataSet<Long> dataSetOutput = flinkExecutor.fee.fromElements(Long.class, count);
        output.accept(dataSetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkCountOperator<>(this.getInputType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.flink.count.load";
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
        return true;
    }

}
