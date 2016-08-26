package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.IntersectOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
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
    public void evaluate(ChannelInstance[] inputs,
                         ChannelInstance[] outputs,
                         JavaExecutor javaExecutor,
                         OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        // Strategy:
        // 1) Create a probing table for the smaller input. This must be a set to deal with duplicates there.
        // 2) Probe the greater input against the table. Remove on probing to deal with duplicates there.

        final CardinalityEstimate cardinalityEstimate0 = this.getInput(0).getCardinalityEstimate();
        final CardinalityEstimate cardinalityEstimate1 = this.getInput(0).getCardinalityEstimate();

        boolean isMaterialize0 = cardinalityEstimate0 != null &&
                cardinalityEstimate1 != null &&
                cardinalityEstimate0.getUpperEstimate() <= cardinalityEstimate1.getUpperEstimate();

        final Stream<Type> candidateStream;
        final Set<Type> probingTable;
        if (isMaterialize0) {
            candidateStream = ((JavaChannelInstance) inputs[0]).provideStream();
            probingTable = this.createProbingTable(((JavaChannelInstance) inputs[1]).provideStream());
        } else {
            candidateStream = ((JavaChannelInstance) inputs[1]).provideStream();
            probingTable = this.createProbingTable(((JavaChannelInstance) inputs[0]).provideStream());
        }

        Stream<Type> intersectStream = candidateStream.filter(probingTable::remove);


        ((StreamChannel.Instance) outputs[0]).accept(intersectStream);
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
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final NestableLoadProfileEstimator estimator = NestableLoadProfileEstimator.parseSpecification(
                configuration.getStringProperty("rheem.java.intersect.load")
        );
        return Optional.of(estimator);
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

    @Override
    public boolean isExecutedEagerly() {
        return true;
    }
}
