package io.rheem.rheem.flink.operators;

import org.apache.flink.api.java.DataSet;
import io.rheem.rheem.basic.data.Tuple2;
import io.rheem.rheem.basic.operators.CartesianOperator;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.util.ReflectionUtils;
import io.rheem.rheem.core.util.Tuple;
import io.rheem.rheem.flink.channels.DataSetChannel;
import io.rheem.rheem.flink.execution.FlinkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Flink implementation of the {@link CartesianOperator}.
 */
public class FlinkCartesianOperator<InputType0, InputType1>
        extends CartesianOperator<InputType0, InputType1>
        implements FlinkExecutionOperator  {

    /**
     * Creates a new instance.
     */
    public FlinkCartesianOperator(DataSetType<InputType0> inputType0, DataSetType<InputType1> inputType1) {
        super(inputType0, inputType1);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkCartesianOperator(CartesianOperator<InputType0, InputType1> that) {
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

        final DataSetChannel.Instance input0 = (DataSetChannel.Instance) inputs[0];
        final DataSetChannel.Instance input1 = (DataSetChannel.Instance) inputs[1];
        final DataSetChannel.Instance output = (DataSetChannel.Instance) outputs[0];


        final DataSet<InputType0> dataSetInput0 = input0.provideDataSet();
        final DataSet<InputType1> dataSetInput1 = input1.provideDataSet();

        final DataSet<Tuple2<InputType0, InputType1>> datasetOutput = dataSetInput0.cross(dataSetInput1).with(
                (dataInput0, dataInput1) -> {
                    return new Tuple2<>(dataInput0, dataInput1);
                }
        ).returns(ReflectionUtils.specify(Tuple2.class));

        output.accept(datasetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkCartesianOperator<>(this.getInputType0(), this.getInputType1());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.cartesian.load";
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
