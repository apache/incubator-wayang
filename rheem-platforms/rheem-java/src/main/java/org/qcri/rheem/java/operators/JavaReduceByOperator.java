package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    public void open(ChannelExecutor[] inputs, FunctionCompiler compiler) {
        final BiFunction<Type, Type, Type> udf = compiler.compile(this.reduceDescriptor);
        JavaExecutor.openFunction(this, udf, inputs);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Function<Type, KeyType> keyExtractor = compiler.compile(this.keyDescriptor);
        final BinaryOperator<Type> reduceFunction = compiler.compile(this.reduceDescriptor);
        JavaExecutor.openFunction(this, reduceFunction, inputs);

        final Map<KeyType, Optional<Type>> reductionResult = inputs[0].<Type>provideStream().collect(
                Collectors.groupingBy(keyExtractor, Collectors.reducing(reduceFunction)));
        final Stream<Type> finishedStream = reductionResult.values().stream()
                .filter(Optional::isPresent)
                .map(Optional::get);

        outputs[0].acceptStream(finishedStream);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaReduceByOperator<>(this.getType(), this.getKeyDescriptor(), this.getReduceDescriptor());
    }
}
