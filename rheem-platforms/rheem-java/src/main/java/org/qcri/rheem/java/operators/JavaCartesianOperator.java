package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.CartesianOperator;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Java implementation of the {@link CartesianOperator}.
 */
public class JavaCartesianOperator<InputType0, InputType1>
        extends CartesianOperator<InputType0, InputType1>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     */
    public JavaCartesianOperator(DataSetType<InputType0> inputType0, DataSetType<InputType1> inputType1) {
        super(inputType0, inputType1);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaCartesianOperator(CartesianOperator<InputType0, InputType1> that) {
        super(that);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        if (inputs.length != 2) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        Collection<ExecutionLineageNode> executionLineageNodes = new LinkedList<>();
        Collection<ChannelInstance> producedChannelInstances = new LinkedList<>();
        ExecutionLineageNode probingExecutionLineageNode = new ExecutionLineageNode(operatorContext);
        probingExecutionLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "rheem.java.cartesian.load.probing", javaExecutor.getConfiguration()
        ));
        ExecutionLineageNode indexingExecutionLineageNode = new ExecutionLineageNode(operatorContext);
        indexingExecutionLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "rheem.java.cartesian.load.indexing", javaExecutor.getConfiguration()
        ));

        StreamChannel.Instance output = (StreamChannel.Instance) outputs[0];
        ChannelInstance materializedInput, probingInput;
        if (inputs[0] instanceof CollectionChannel.Instance) {
            final Collection<InputType0> collection = ((CollectionChannel.Instance) inputs[0]).provideCollection();
            final Stream<InputType1> stream = ((JavaChannelInstance) inputs[1]).provideStream();
            output.<Tuple2<InputType0, InputType1>>accept(
                    stream.flatMap(e1 -> collection.stream().map(
                            e0 -> new Tuple2<>(e0, e1)
                    ))
            );
            materializedInput = inputs[0];
            probingInput = inputs[1];
            probingExecutionLineageNode.addPredecessor(materializedInput.getLineage());

        } else if (inputs[1] instanceof CollectionChannel.Instance) {
            final Stream<InputType0> stream = ((JavaChannelInstance) inputs[0]).provideStream();
            final Collection<InputType1> collection = ((CollectionChannel.Instance) inputs[1]).provideCollection();
            output.<Tuple2<InputType0, InputType1>>accept(
                    stream.flatMap(e0 -> collection.stream().map(
                            e1 -> new Tuple2<>(e0, e1)
                    ))
            );
            materializedInput = inputs[1];
            probingInput = inputs[0];
            probingExecutionLineageNode.addPredecessor(materializedInput.getLineage());

        } else if (operatorContext.getInputCardinality(0).getGeometricMeanEstimate() <= operatorContext.getInputCardinality(1).getGeometricMeanEstimate()) {
            // Fallback: Materialize one side.
            final Collection<InputType0> collection = (Collection<InputType0>) ((JavaChannelInstance) inputs[0]).provideStream().collect(Collectors.toList());
            final Stream<InputType1> stream = ((JavaChannelInstance) inputs[1]).provideStream();
            output.<Tuple2<InputType0, InputType1>>accept(
                    stream.flatMap(e1 -> collection.stream().map(
                            e0 -> new Tuple2<>(e0, e1)
                    ))
            );
            materializedInput = inputs[0];
            probingInput = inputs[1];
            indexingExecutionLineageNode.addPredecessor(materializedInput.getLineage());
            indexingExecutionLineageNode.collectAndMark(executionLineageNodes, producedChannelInstances);

        } else {
            final Collection<InputType1> collection = (Collection<InputType1>) ((JavaChannelInstance) inputs[1]).provideStream().collect(Collectors.toList());
            final Stream<InputType0> stream = ((JavaChannelInstance) inputs[0]).provideStream();
            output.<Tuple2<InputType0, InputType1>>accept(
                    stream.flatMap(e0 -> collection.stream().map(
                            e1 -> new Tuple2<>(e0, e1)
                    ))
            );
            materializedInput = inputs[1];
            probingInput = inputs[0];
            indexingExecutionLineageNode.addPredecessor(materializedInput.getLineage());
            indexingExecutionLineageNode.collectAndMark(executionLineageNodes, producedChannelInstances);
        }

        probingExecutionLineageNode.addPredecessor(probingInput.getLineage());
        output.getLineage().addPredecessor(probingExecutionLineageNode);
        return new Tuple<>(executionLineageNodes, producedChannelInstances);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("rheem.java.cartesian.load.indexing", "rheem.java.cartesian.load.probing");
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaCartesianOperator<>(this.getInputType0(), this.getInputType1());
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

}
