package org.qcri.rheem.flink.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.ZipWithIdOperator;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.execution.FlinkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Flink implementation of the {@link MapOperator}.
 */
public class FlinkZipWithIdOperator<InputType>
        extends ZipWithIdOperator<InputType>
        implements FlinkExecutionOperator {

    /**
     * Creates a new instance.
     */
    public FlinkZipWithIdOperator(DataSetType<InputType> inputType) {
        super(inputType);
    }

    /**
     * Creates a new instance.
     */
    public FlinkZipWithIdOperator(Class<InputType> inputTypeClass) {
        super(inputTypeClass);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkZipWithIdOperator(ZipWithIdOperator<InputType> that) {
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
        final DataSet<org.apache.flink.api.java.tuple.Tuple2<Long, InputType>>
                dataSetZipped = DataSetUtils.zipWithUniqueId(dataSetInput);

        final DataSet<Tuple2<Long, InputType>> dataSetOutput = dataSetZipped.map(pair -> new Tuple2<>(pair.f0, pair.f1)).returns(ReflectionUtils.specify(Tuple2.class));

        output.accept(dataSetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkZipWithIdOperator<>(this.getInputType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.zipwithid.load";
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
