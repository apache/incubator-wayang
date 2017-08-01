package org.qcri.rheem.flink.operators;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.core.function.ConsumerDescriptor;
import org.qcri.rheem.core.function.FunctionDescriptor.SerializableConsumer;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.execution.FlinkExecutor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Implementation of the {@link LocalCallbackSink} operator for the Flink platform.
 */
public class FlinkLocalCallbackSink <T extends Serializable> extends LocalCallbackSink<T> implements FlinkExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param callback callback that is executed locally for each incoming data unit
     * @param type     type of the incoming elements
     */
    public FlinkLocalCallbackSink(ConsumerDescriptor.SerializableConsumer<T> callback, DataSetType type) {
        super(callback, type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkLocalCallbackSink(LocalCallbackSink<T> that) {
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
        final DataSet<T> inputDataSet = input.provideDataSet();


        if(this.collector != null){
            inputDataSet.output(new LocalCollectionOutputFormat<T>(this.collector));
        }else{
            //inputDataSet.output(flinkExecutor.getCompiler().compile( this.getCallbackDescriptor()));
            inputDataSet.output(new PrintingOutputFormat<T>());
        }


        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkLocalCallbackSink<T>(this);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.localcallbacksink.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have output channels.", this));
    }

    @Override
    public boolean containsAction() {
        return true;
    }

}
