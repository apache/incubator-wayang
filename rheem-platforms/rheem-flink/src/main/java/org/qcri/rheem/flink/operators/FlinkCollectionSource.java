package org.qcri.rheem.flink.operators;

import org.apache.flink.api.java.DataSet;
import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.execution.FlinkExecutor;
import org.qcri.rheem.java.channels.CollectionChannel;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * This is execution operator implements the {@link CollectionSource}.
 */
public class FlinkCollectionSource<Type> extends CollectionSource<Type> implements FlinkExecutionOperator{
    public FlinkCollectionSource(DataSetType<Type> type) {
        this(null, type);
    }

    public FlinkCollectionSource(Collection<Type> collection, DataSetType<Type> type) {
        super(collection, type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkCollectionSource(CollectionSource<Type> that) {
        super(that);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == 0;
        assert outputs.length == 1;

        final Collection<Type> collection;
        if (this.collection != null) {
            collection = this.collection;
        } else {
            collection = ((CollectionChannel.Instance)inputs[0]).provideCollection();
        }

        final DataSet<Type> datasetOutput = flinkExecutor.fee.fromCollection(collection);
        ((DataSetChannel.Instance) outputs[0]).accept(datasetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }


    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.collectionsource.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkCollectionSource<>(this.getCollection(), this.getType());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

}
