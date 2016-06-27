package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.*;

/**
 * Java implementation of the {@link JavaReservoirSampleOperator}.
 */
public class JavaReservoirSampleOperator<Type>
        extends SampleOperator<Type>
        implements JavaExecutionOperator {

    Random rand;

    /**
     * Creates a new instance.
     *
     * @param sampleSize
     */
    public JavaReservoirSampleOperator(Integer sampleSize, DataSetType type) {
        super(sampleSize, type, Methods.RESERVOIR);
        rand = new Random();
    }

    /**
     * Creates a new instance.
     *
     * @param sampleSize
     * @param datasetSize
     */
    public JavaReservoirSampleOperator(Integer sampleSize, Long datasetSize, DataSetType type) {
        super(sampleSize, datasetSize, type, Methods.RESERVOIR);
        rand = new Random();
    }


    @Override
    @SuppressWarnings("unchecked")
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        ((CollectionChannel.Instance) outputs[0]).accept(reservoirSample(rand, ((JavaChannelInstance) inputs[0]).<Type>provideStream().iterator(), sampleSize));
    }

    private static <T> List<T> reservoirSample(Random rand, Iterator<T> items, long m){
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
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(Configuration configuration) {
        return Optional.of(new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(this.getNumInputs(), 1, 0.9d, (inCards, outCards) -> 25 * inCards[0] + 350000),
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
