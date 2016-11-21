package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.basic.operators.UDFSampleSize;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutionContext;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.*;

/**
 * Java implementation of the {@link JavaReservoirSampleOperator}.
 */
public class JavaReservoirSampleOperator<Type>
        extends SampleOperator<Type>
        implements JavaExecutionOperator {

    private final Random rand = new Random(seed);

    /**
     * Creates a new instance.
     *
     * @param sampleSize
     */
    public JavaReservoirSampleOperator(int sampleSize, DataSetType<Type> type) {
        super(sampleSize, type, Methods.RESERVOIR);
    }

    /**
     * Creates a new instance.
     *
     * @param udfSampleSize
     */
    public JavaReservoirSampleOperator(UDFSampleSize udfSampleSize, DataSetType<Type> type) {
        super(udfSampleSize, type, Methods.RESERVOIR);
    }

    /**
     * Creates a new instance.
     *
     * @param sampleSize
     * @param datasetSize
     */
    public JavaReservoirSampleOperator(int sampleSize, long datasetSize, DataSetType<Type> type) {
        super(sampleSize, datasetSize, type, Methods.RESERVOIR);
    }

    /**
     * Creates a new instance.
     *
     * @param udfSampleSize
     * @param datasetSize
     */
    public JavaReservoirSampleOperator(UDFSampleSize udfSampleSize, long datasetSize, DataSetType<Type> type) {
        super(udfSampleSize, datasetSize, type, Methods.RESERVOIR);
    }

    /**
     * Creates a new instance.
     *
     * @param sampleSize
     * @param datasetSize
     * @param seed
     */
    public JavaReservoirSampleOperator(int sampleSize, long datasetSize, long seed, DataSetType<Type> type) {
        super(sampleSize, datasetSize, seed, type, Methods.RESERVOIR);
    }

    /**
     * Creates a new instance.
     *
     * @param udfSampleSize
     * @param datasetSize
     * @param seed
     */
    public JavaReservoirSampleOperator(UDFSampleSize udfSampleSize, long datasetSize, long seed, DataSetType<Type> type) {
        super(udfSampleSize, datasetSize, seed, type, Methods.RESERVOIR);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaReservoirSampleOperator(SampleOperator<Type> that) {
        super(that);
        assert that.getSampleMethod() == Methods.RESERVOIR || that.getSampleMethod() == Methods.ANY;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<OptimizationContext.OperatorContext>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        if (udfSampleSize != UNKNOWN_UDF_SAMPLE_SIZE) { //if it is not null, compute the sample size with the UDF
            int iterationNumber = operatorContext.getOptimizationContext().getIterationNumber();
            udfSampleSize.open(new JavaExecutionContext(this, inputs, iterationNumber));
            sampleSize = udfSampleSize.apply();
        }

        ((CollectionChannel.Instance) outputs[0]).accept(reservoirSample(rand, ((JavaChannelInstance) inputs[0]).<Type>provideStream().iterator(), sampleSize));

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    private static <T> List<T> reservoirSample(Random rand, Iterator<T> items, long m) {
        ArrayList<T> res = new ArrayList<T>(Math.toIntExact(m));
        int count = 0;
        while (items.hasNext()) {
            T item = items.next();
            count++;
            if (count <= m)
                res.add(item);
            else {
                int r = rand.nextInt(count);
                if (r < m)
                    res.set(r, item);
            }
        }
        return res;
    }

    @Override
    public Optional<LoadProfileEstimator<ExecutionOperator>> createLoadProfileEstimator(Configuration configuration) {
        return Optional.of(new NestableLoadProfileEstimator<>(
                new DefaultLoadEstimator<>(this.getNumInputs(), 1, 0.9d, (inCards, outCards) -> 25 * inCards[0] + 350000),
                LoadEstimator.createFallback(this.getNumInputs(), 1)
        ));
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaReservoirSampleOperator<>(this.sampleSize, this.getType());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

}
