package org.qcri.rheem.flink.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.FunctionDescriptor;
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
import org.qcri.rheem.flink.execution.FlinkExecutionContext;
import org.qcri.rheem.flink.execution.FlinkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Flink implementation of the {@link FlatMapOperator}.
 */
public class FlinkFlatMapOperator<InputType, OutputType>
        extends FlatMapOperator<InputType, OutputType>
        implements FlinkExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param functionDescriptor
     */
    public FlinkFlatMapOperator(DataSetType<InputType> inputType, DataSetType<OutputType> outputType,
                                FlatMapDescriptor<InputType, OutputType> functionDescriptor) {
        super(functionDescriptor, inputType, outputType);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkFlatMapOperator(FlatMapOperator<InputType, OutputType> that) {
        super(that);
    }

    public FlinkFlatMapOperator(){
        super((FlatMapOperator<InputType, OutputType>) null);
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


        final DataSet<InputType>  dataSetInput  = input.provideDataSet();

        final DataSet<OutputType> dataSetOutput;
        if( this.getNumBroadcastInputs() > 0 ){

            Tuple<String, DataSet> names = searchBroadcast(inputs);

            FlinkExecutionContext fex = new FlinkExecutionContext(this, inputs, 0);

            RichFlatMapFunction<InputType, OutputType> richFunction = flinkExecutor.compiler
                    .compile(
                            (FunctionDescriptor.ExtendedSerializableFunction)
                                    this.functionDescriptor.getJavaImplementation(),
                            fex
                    );
            fex.setRichFunction(richFunction);

            dataSetOutput = dataSetInput
                    .flatMap(richFunction)
                    .returns(this.functionDescriptor.getOutputType().getTypeClass())
                    .withBroadcastSet(names.field1, names.field0);

        }else {

            final FlatMapFunction<InputType, OutputType> flatMapFunction =
                    flinkExecutor.getCompiler().compile((FunctionDescriptor.SerializableFunction<InputType, Iterable<OutputType>>)this.functionDescriptor.getJavaImplementation());

            dataSetOutput = dataSetInput.flatMap(flatMapFunction).returns(this.functionDescriptor.getOutputType().getTypeClass());
        }
        output.accept(dataSetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    private Tuple<String, DataSet> searchBroadcast(ChannelInstance[] inputs) {
        for(int i = 0; i < this.inputSlots.length; i++){
            if( this.inputSlots[i].isBroadcast() ){
                DataSetChannel.Instance dataSetChannel = (DataSetChannel.Instance)inputs[inputSlots[i].getIndex()];
                return new Tuple<>(inputSlots[i].getName(), dataSetChannel.provideDataSet());
            }
        }
        return null;
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkFlatMapOperator<>(this.getInputType(), this.getOutputType(), this.getFunctionDescriptor());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.flatmap.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                FlinkExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.functionDescriptor, configuration);
        return optEstimator;
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
