package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Java implementation of the {@link ReduceByOperator}.
 */
public class JavaReduceByOperator<Type, KeyType>
        extends ReduceByOperator<Type, KeyType>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type             type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param keyDescriptor    describes how to extract the key from data units
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public JavaReduceByOperator(DataSetType<Type> type, TransformationDescriptor<Type, KeyType> keyDescriptor,
                                ReduceDescriptor<Type> reduceDescriptor) {
        super(keyDescriptor, reduceDescriptor, type);
    }

    @Override
    public void open(JavaChannelInstance[] inputs, FunctionCompiler compiler) {
        final BiFunction<Type, Type, Type> udf = compiler.compile(this.reduceDescriptor);
        JavaExecutor.openFunction(this, udf, inputs);
    }

    @Override
    public void evaluate(JavaChannelInstance[] inputs, JavaChannelInstance[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Function<Type, KeyType> keyExtractor = compiler.compile(this.keyDescriptor);
        final BinaryOperator<Type> reduceFunction = compiler.compile(this.reduceDescriptor);
        JavaExecutor.openFunction(this, reduceFunction, inputs);

        final Map<KeyType, Type> reductionResult = inputs[0].<Type>provideStream().collect(
                Collectors.groupingBy(keyExtractor, new ReducingCollector<>(reduceFunction))
        );
        ((CollectionChannel.Instance) outputs[0]).accept(reductionResult.values());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(Configuration configuration) {
        final NestableLoadProfileEstimator operatorEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> 700 * inputCards[0] + 1040 * outputCards[0] + 1100000),
                new DefaultLoadEstimator(1, 1, 0, (inputCards, outputCards) -> 0)
        );

        final LoadProfileEstimator keyEstimator =
                configuration.getFunctionLoadProfileEstimatorProvider().provideFor(this.getKeyDescriptor());
        operatorEstimator.nest(keyEstimator);

        final LoadProfileEstimator reduceDescriptor =
                configuration.getFunctionLoadProfileEstimatorProvider().provideFor(this.getReduceDescriptor());
        operatorEstimator.nest(reduceDescriptor);

        return Optional.of(operatorEstimator);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaReduceByOperator<>(this.getType(), this.getKeyDescriptor(), this.getReduceDescriptor());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        if (this.getInput(index).isBroadcast()) return Collections.singletonList(CollectionChannel.DESCRIPTOR);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

    /**
     * Immitates {@link Collectors#reducing(BinaryOperator)} but assumes that there is always at least one element
     * in each reduction.
     */
    private static class ReducingCollector<T> implements Collector<T, List<T>, T> {

        private final BinaryOperator<T> reduceFunction;

        ReducingCollector(BinaryOperator<T> reduceFunction) {
            this.reduceFunction = reduceFunction;
        }

        @Override
        public Supplier<List<T>> supplier() {
            return () -> new ArrayList<>(1);
        }

        @Override
        public BiConsumer<List<T>, T> accumulator() {
            return (list, element) -> {
                if (list.isEmpty()) {
                    list.add(element);
                } else {
                    list.set(0, this.reduceFunction.apply(list.get(0), element));
                }
            };
        }

        @Override
        public BinaryOperator<List<T>> combiner() {
            return (list1, list2) -> {
                if (list1.isEmpty()) {
                    return list2;
                } else if (list2.isEmpty()) {
                    return list2;
                } else {
                    list1.set(0, this.reduceFunction.apply(list1.get(0), list2.get(0)));
                    return list1;
                }
            };
        }

        @Override
        public Function<List<T>, T> finisher() {
            return list -> {
                assert !list.isEmpty();
                return list.get(0);
            };
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Collections.emptySet();
        }
    }
}
