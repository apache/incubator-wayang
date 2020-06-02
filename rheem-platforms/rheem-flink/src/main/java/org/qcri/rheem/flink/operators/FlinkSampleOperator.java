package org.qcri.rheem.flink.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.execution.FlinkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;

/**
 * Flink implementation of the {@link SampleOperator}. Sampling with replacement (i.e., the sample may contain duplicates)
 */
public class FlinkSampleOperator<Type>
        extends SampleOperator<Type>
        implements FlinkExecutionOperator {
    private Random rand;


    /**
     * Creates a new instance.
     */
    public FlinkSampleOperator(IntUnaryOperator sampleSizeFunction, DataSetType<Type> type, LongUnaryOperator seedFunction) {
        super(sampleSizeFunction, type, Methods.RANDOM, seedFunction);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkSampleOperator(SampleOperator<Type> that) {
        super(that);
        assert that.getSampleMethod() == Methods.RANDOM
                || that.getSampleMethod() == Methods.BERNOULLI
                || that.getSampleMethod() == Methods.RESERVOIR
                || that.getSampleMethod() == Methods.ANY;
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


        long size = Long.MAX_VALUE;
      /*  try {
            size = dataSetInput.count();
        } catch (Exception e) {
            throw new RheemException(e);
        }*/
        DataSet<Type> dataSetOutput;

        int sampleSize = this.getSampleSize(operatorContext);
        long seed = this.getSeed(operatorContext);

        if(this.getSampleSize(operatorContext) >= size){
            dataSetOutput = dataSetInput;
        }else {
            double faction = (size / sampleSize) + 0.01d;
            switch (this.getSampleMethod()) {
                case RANDOM:
                    dataSetOutput = DataSetUtils.sampleWithSize(dataSetInput, true, sampleSize, seed);
                    break;
                case ANY:
                    Random rand = new Random(seed);
                    dataSetOutput = dataSetInput.filter(a -> {return rand.nextBoolean();}).first(sampleSize);
                    break;
                case BERNOULLI:
                    dataSetOutput = DataSetUtils.sample(dataSetInput, false, faction, seed).first(sampleSize);
                    break;
                case RESERVOIR:
                    dataSetOutput = DataSetUtils.sampleWithSize(dataSetInput, true, sampleSize, seed);
                    break;
                default:
                    throw new RheemException("The option is not valid");
            }
        }

        // assuming the sample is small better use a collection instance, the optimizer can transform the output if necessary
        output.accept(dataSetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkSampleOperator<Type>(this);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.sample.load";
    }


    @Override
    public boolean containsAction() {
        return true;
    }


}
