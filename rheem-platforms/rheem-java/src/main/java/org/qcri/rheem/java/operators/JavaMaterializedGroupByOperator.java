package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.MaterializedGroupByOperator;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Java implementation of the {@link MaterializedGroupByOperator}.
 */
public class JavaMaterializedGroupByOperator<Type, KeyType>
        extends MaterializedGroupByOperator<Type, KeyType>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type          type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param keyDescriptor describes how to extract the key from data units
     */
    public JavaMaterializedGroupByOperator(DataSetType<Type> type, TransformationDescriptor<Type, KeyType> keyDescriptor) {
        super(type, keyDescriptor);
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final Stream<Type> inputStream = inputStreams[0];
        final Function<Type, KeyType> keyExtractor = compiler.compile(this.keyDescriptor);
        final Map<KeyType, List<Type>> collocation = inputStream.collect(
                Collectors.groupingBy(
                        keyExtractor,
                        Collectors.toList())); // Not sure if this is thread-safe... Will we use #parallelStream()?

        return new Stream[]{collocation.values().stream()};
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaMaterializedGroupByOperator<>(this.getType(), this.getKeyDescriptor());
    }
}
