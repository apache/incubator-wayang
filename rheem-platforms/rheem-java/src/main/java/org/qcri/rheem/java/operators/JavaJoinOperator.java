package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;

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
    public JavaJoinOperator(DataSetType<InputType0> inputType0,
                            DataSetType<InputType1> inputType1,
                            TransformationDescriptor<InputType0, KeyType> keyDescriptor0,
                            TransformationDescriptor<InputType1, KeyType> keyDescriptor1) {

        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaJoinOperator(JoinOperator<InputType0, InputType1, KeyType> that) {
        super(that);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs,
                         ChannelInstance[] outputs,
                         JavaExecutor javaExecutor,
                         OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Function<InputType0, KeyType> keyExtractor0 = javaExecutor.getCompiler().compile(this.keyDescriptor0);
        final Function<InputType1, KeyType> keyExtractor1 = javaExecutor.getCompiler().compile(this.keyDescriptor1);

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
            ((JavaChannelInstance) inputs[0]).<InputType0>provideStream().forEach(dataQuantum0 ->
                    probeTable.compute(keyExtractor0.apply(dataQuantum0),
                            (key, value) -> {
                                value = value == null ? new LinkedList<>() : value;
                                value.add(dataQuantum0);
                                return value;
                            }
                    )
            );
            joinStream = ((JavaChannelInstance) inputs[1]).<InputType1>provideStream().flatMap(dataQuantum1 ->
                    probeTable.getOrDefault(keyExtractor1.apply(dataQuantum1), Collections.emptyList()).stream()
                            .map(dataQuantum0 -> new Tuple2<>(dataQuantum0, dataQuantum1)));

        } else {
            final int expectedNumElements = cardinalityEstimate1 == null ?
                    1000 :
                    (int) (cardinalityEstimate1.getUpperEstimate() - cardinalityEstimate1.getLowerEstimate()) / 2;
            Map<KeyType, Collection<InputType1>> probeTable = new HashMap<>(expectedNumElements);
            ((JavaChannelInstance) inputs[1]).<InputType1>provideStream().forEach(dataQuantum1 ->
                    probeTable.compute(keyExtractor1.apply(dataQuantum1),
                            (key, value) -> {
                                value = value == null ? new LinkedList<>() : value;
                                value.add(dataQuantum1);
                                return value;
                            }
                    )
            );
            joinStream = ((JavaChannelInstance) inputs[0]).<InputType0>provideStream().flatMap(dataQuantum0 ->
                    probeTable.getOrDefault(keyExtractor0.apply(dataQuantum0), Collections.emptyList()).stream()
                            .map(dataQuantum1 -> new Tuple2<>(dataQuantum0, dataQuantum1)));
        }

        ((StreamChannel.Instance) outputs[0]).accept(joinStream);
    }

    @Override
    public Optional<LoadProfileEstimator<ExecutionOperator>> createLoadProfileEstimator(Configuration configuration) {
        final NestableLoadProfileEstimator estimator = LoadProfileEstimators.createFromJuelSpecification(
                configuration.getStringProperty("rheem.java.join.load")
        );
        return Optional.of(estimator);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaJoinOperator<>(this.getInputType0(), this.getInputType1(),
                this.getKeyDescriptor0(), this.getKeyDescriptor1());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    @Override
    public boolean isExecutedEagerly() {
        return false;
    }
}
