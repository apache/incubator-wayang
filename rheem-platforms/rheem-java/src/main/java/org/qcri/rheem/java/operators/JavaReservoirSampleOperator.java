package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Java implementation of the {@link JavaReservoirSampleOperator}.
 */
public class JavaReservoirSampleOperator<Type>
        extends SampleOperator<Type>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param sampleSize size of sample
     */
    public JavaReservoirSampleOperator(int sampleSize, PredicateDescriptor<Type> predicateDescriptor) {
        super(sampleSize, predicateDescriptor);
    }

    public JavaReservoirSampleOperator(int sampleSize, PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor, Class<Type> typeClass) {
        super(sampleSize, predicateDescriptor, typeClass);
    }

    /**
     * Creates a new instance.
     *
     * @param sampleSize
     */
    public JavaReservoirSampleOperator(int sampleSize, long totalSize, DataSetType type) {
        super(sampleSize, totalSize, type);
    }

    @Override
    public void open(ChannelExecutor[] inputs, FunctionCompiler compiler) {
        final Predicate<Type> filterFunction = compiler.compile(this.predicateDescriptor);
        JavaExecutor.openFunction(this, filterFunction, inputs);
    }

    int count = 0;
    @Override
    @SuppressWarnings("unchecked")
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final List<Type> initList = (List) inputs[0].<Type>provideStream().collect(Collectors.toList());
        if (sampleSize == 0 && sampleFraction != 0.0) // if fraction was given as input
            sampleSize = (int) Math.round(sampleFraction * initList.size());
        outputs[0].acceptStream(reservoirSample(rand, initList, sampleSize).stream());
    }

    private static <T> List<T> reservoirSample(Random rand, Iterable<T> items, long m){
        ArrayList<T> res = new ArrayList<T>(Math.toIntExact(m));
        int count = 0;
        for (T item : items) {
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
        return new JavaReservoirSampleOperator<>(this.sampleSize, this.getPredicateDescriptor());
    }
}
