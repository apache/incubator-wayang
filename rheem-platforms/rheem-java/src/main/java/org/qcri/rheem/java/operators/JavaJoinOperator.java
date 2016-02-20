package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Java implementation of the {@link JoinOperator}.
 */
public class JavaJoinOperator<InputType0, InputType1, KeyType>
        extends JoinOperator<InputType0, InputType1, KeyType>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     */
    public JavaJoinOperator(DataSetType<InputType0> inputType0, DataSetType inputType1,
                            TransformationDescriptor<InputType0, KeyType> keyDescriptor0,
                            TransformationDescriptor<InputType1, KeyType> keyDescriptor1) {

        super(inputType0, inputType1, keyDescriptor0, keyDescriptor1);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Function<InputType0, KeyType> keyExtractor0 = compiler.compile(this.keyDescriptor0);
        final Function<InputType1, KeyType> keyExtractor1 = compiler.compile(this.keyDescriptor1);

        final CardinalityEstimate cardinalityEstimate0 = this.getInput(0).getCardinalityEstimate();
        final CardinalityEstimate cardinalityEstimate1 = this.getInput(0).getCardinalityEstimate();

        final Stream<Tuple2<InputType0, InputType1>> joinStream;

        boolean isMaterialize0 = cardinalityEstimate0 != null &&
                cardinalityEstimate1 != null &&
                cardinalityEstimate0.getUpperEstimate() <= cardinalityEstimate1.getUpperEstimate();

        if (isMaterialize0) {
            final int expectedNumElements =
                    (int) (cardinalityEstimate0.getUpperEstimate() - cardinalityEstimate0.getLowerEstimate()) / 2;
            Map<KeyType, Collection<InputType0>> probeTable = new HashMap<>(expectedNumElements);
            inputs[0].<InputType0>provideStream().forEach(dataQuantum0 ->
                    probeTable.compute(keyExtractor0.apply(dataQuantum0),
                            (key, value) -> {
                                value = value == null ? new LinkedList<>() : value;
                                value.add(dataQuantum0);
                                return value;
                            }
                    )
            );
            joinStream = inputs[1].<InputType1>provideStream().flatMap(dataQuantum1 ->
                    probeTable.getOrDefault(keyExtractor1.apply(dataQuantum1), Collections.emptyList()).stream()
                            .map(dataQuantum0 -> new Tuple2<>(dataQuantum0, dataQuantum1)));

        } else {
            final int expectedNumElements = cardinalityEstimate1 == null ?
                    1000 :
                    (int) (cardinalityEstimate1.getUpperEstimate() - cardinalityEstimate1.getLowerEstimate()) / 2;
            Map<KeyType, Collection<InputType1>> probeTable = new HashMap<>(expectedNumElements);
            inputs[1].<InputType1>provideStream().forEach(dataQuantum1 ->
                    probeTable.compute(keyExtractor1.apply(dataQuantum1),
                            (key, value) -> {
                                value = value == null ? new LinkedList<>() : value;
                                value.add(dataQuantum1);
                                return value;
                            }
                    )
            );
            joinStream = inputs[0].<InputType0>provideStream().flatMap(dataQuantum0 ->
                    probeTable.getOrDefault(keyExtractor0.apply(dataQuantum0), Collections.emptyList()).stream()
                            .map(dataQuantum1 -> new Tuple2<>(dataQuantum0, dataQuantum1)));
        }

        outputs[0].acceptStream(joinStream);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaJoinOperator<>(this.getInputType0(), this.getInputType1(),
                this.getKeyDescriptor0(), this.getKeyDescriptor1());
    }
}
