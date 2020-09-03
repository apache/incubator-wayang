package io.rheem.rheem.java.operators;

import io.rheem.rheem.basic.operators.CollectionSource;
import io.rheem.rheem.basic.operators.TextFileSource;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.util.Tuple;
import io.rheem.rheem.java.channels.CollectionChannel;
import io.rheem.rheem.java.execution.JavaExecutor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This is execution operator implements the {@link TextFileSource}.
 */
public class JavaCollectionSource<T> extends CollectionSource<T> implements JavaExecutionOperator {

    public JavaCollectionSource(Collection<T> collection, DataSetType<T> type) {
        super(collection, type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaCollectionSource(CollectionSource<T> that) {
        super(that);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == 0;
        assert outputs.length == 1;
        ((CollectionChannel.Instance) outputs[0]).accept(this.getCollection());

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }


    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.collectionsource.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaCollectionSource<>(this.getCollection(), this.getType());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not support input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

}
