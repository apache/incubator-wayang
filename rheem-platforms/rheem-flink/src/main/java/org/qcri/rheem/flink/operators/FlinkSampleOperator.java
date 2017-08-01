package org.qcri.rheem.flink.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.execution.FlinkExecutor;
import org.qcri.rheem.java.channels.CollectionChannel;
import scala.collection.JavaConversions;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
        assert that.getSampleMethod() == Methods.RANDOM || that.getSampleMethod() == Methods.ANY;
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

        PredicateDescriptor.SerializablePredicate<Type> filter;
        switch (this.getSampleMethod()) {
            case RANDOM:
            case ANY:
                Random random_any = new Random(this.getSeed(operatorContext));
                filter = dataquantum -> random_any.nextLong() % 2 == 0;
                break;
            case BERNOULLI:
                Random random_bernoulli = new Random(this.getSeed(operatorContext));
                filter = dataquantum -> random_bernoulli.nextLong() % 100 == 0;
                break;
            default:
                throw new RheemException("The option is bad");
        }


        final DataSet<Type> dataSetOutput =  dataSetInput.filter(flinkExecutor.compiler.compile(filter));

        // assuming the sample is small better use a collection instance, the optimizer can transform the output if necessary
        output.accept(dataSetOutput, flinkExecutor);

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkSampleOperator<>(this);
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
