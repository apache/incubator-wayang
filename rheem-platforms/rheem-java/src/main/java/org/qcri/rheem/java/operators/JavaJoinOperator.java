package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.core.function.KeyExtractorDescriptor;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Java implementation of the {@link JoinOperator}.
 */
public class JavaJoinOperator<InputType0, InputType1, KeyType>
        extends JoinOperator<InputType0, InputType1, KeyType>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     *
     */
    public JavaJoinOperator(DataSetType <InputType0> inputType0, DataSetType inputType1,
                            KeyExtractorDescriptor<InputType0, KeyType> keyDescriptor0,
                            KeyExtractorDescriptor<InputType1, KeyType> keyDescriptor1) {

        super(inputType0, inputType1, keyDescriptor0, keyDescriptor1);
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 2) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }
        final Stream<InputType0> inputStream0 = (Stream<InputType0>) inputStreams[0];
        final Stream<InputType1> inputStream1 = (Stream<InputType1>) inputStreams[1];

        final Function<InputType0, KeyType> keyExtractor0 = compiler.compile(this.keyDescriptor0);
        final Function<InputType1, KeyType> keyExtractor1 = compiler.compile(this.keyDescriptor1);


        // Hack to avoid consuming stream more than once, collect it as a list.
        // TODO remove this hack
        final List<InputType1> input1 = inputStream1.collect(Collectors.toList());
        final Stream outputStream =
                inputStream0.flatMap(e0 -> input1.stream()
                        .filter(e1 -> Objects.equals(keyExtractor0.apply(e0), keyExtractor1.apply(e1)))
                        .map(e1 -> new Tuple2<>(e0, e1)));

        return new Stream[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaCartesianOperator<>(getInputType0(), getInputType1());
    }
}
