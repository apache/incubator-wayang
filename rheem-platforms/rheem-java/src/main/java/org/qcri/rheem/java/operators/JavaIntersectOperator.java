package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.IntersectOperator;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Java implementation of the {@link IntersectOperator}.
 */
public class JavaIntersectOperator<Type>
        extends IntersectOperator<Type>
        implements JavaExecutionOperator {

    public JavaIntersectOperator(DataSetType<Type> dataSetType) {
        super(dataSetType);
    }

    public JavaIntersectOperator(Class<Type> typeClass) {
        super(typeClass);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaIntersectOperator(IntersectOperator<Type> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        // Strategy:
        // 1) Create a probing table for the smaller input. This must be a set to deal with duplicates there.
        // 2) Probe the greater input against the table. Remove on probing to deal with duplicates there.

        final CardinalityEstimate cardinalityEstimate0 = operatorContext.getInputCardinality(0);
        final CardinalityEstimate cardinalityEstimate1 = operatorContext.getOutputCardinality(0);

        ExecutionLineageNode indexingExecutionLineageNode = new ExecutionLineageNode(operatorContext);
        indexingExecutionLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "rheem.java.intersect.load.indexing", javaExecutor.getConfiguration()
        ));
        ExecutionLineageNode probingExecutionLineageNode = new ExecutionLineageNode(operatorContext);
        probingExecutionLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "rheem.java.intersect.load.probing", javaExecutor.getConfiguration()
        ));

        boolean isMaterialize0 = cardinalityEstimate0 != null &&
                cardinalityEstimate1 != null &&
                cardinalityEstimate0.getUpperEstimate() <= cardinalityEstimate1.getUpperEstimate();

        final Collection<ExecutionLineageNode> executionLineageNodes = new LinkedList<>();
        final Collection<ChannelInstance> producedChannelInstances = new LinkedList<>();
        final Stream<Type> candidateStream;
        final Set<Type> probingTable;
        if (isMaterialize0) {
            candidateStream = ((JavaChannelInstance) inputs[0]).provideStream();
            probingTable = this.createProbingTable(((JavaChannelInstance) inputs[1]).provideStream());
            indexingExecutionLineageNode.addPredecessor(inputs[0].getLineage());
            probingExecutionLineageNode.addPredecessor(inputs[1].getLineage());
        } else {
            candidateStream = ((JavaChannelInstance) inputs[1]).provideStream();
            probingTable = this.createProbingTable(((JavaChannelInstance) inputs[0]).provideStream());
            indexingExecutionLineageNode.addPredecessor(inputs[1].getLineage());
            probingExecutionLineageNode.addPredecessor(inputs[0].getLineage());
        }

        Stream<Type> intersectStream = candidateStream.filter(probingTable::remove);
        ((StreamChannel.Instance) outputs[0]).accept(intersectStream);
        outputs[0].getLineage().addPredecessor(probingExecutionLineageNode);

        indexingExecutionLineageNode.collectAndMark(executionLineageNodes, producedChannelInstances);
        return new Tuple<>(executionLineageNodes, producedChannelInstances);
    }

    /**
     * Creates a new probing table. The can be altered then.
     *
     * @param stream for that the probing table should be created
     * @return the probing table
     */
    private Set<Type> createProbingTable(Stream<Type> stream) {
        return stream.collect(Collectors.toSet());
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("rheem.java.intersect.load.indexing", "rheem.java.intersect.load.probing");
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaIntersectOperator<>(this.getType());
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
