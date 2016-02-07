package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Map;
import java.util.Optional;
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
     * @param type        type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param keyDescriptor    describes how to extract the key from data units
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public JavaReduceByOperator(DataSetType<Type> type, TransformationDescriptor<Type, KeyType> keyDescriptor,
                                ReduceDescriptor<Type> reduceDescriptor) {
        super(type, keyDescriptor, reduceDescriptor);
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final Stream<Type> inputStream = inputStreams[0];
        final Function<Type, KeyType> keyExtractor = compiler.compile(this.keyDescriptor);
        final BinaryOperator<Type> reduceFunction = compiler.compile(this.reduceDescriptor);
        final Map<KeyType, Optional<Type>> reductionResult = inputStream.collect(
                Collectors.groupingBy(
                        keyExtractor,
                        Collectors.reducing(reduceFunction)));

        return new Stream[]{ reductionResult.values().stream()
            .filter(Optional::isPresent)
            .map(Optional::get)
        };
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaReduceByOperator<>(getType(), getKeyDescriptor(), getReduceDescriptor());
    }
}
