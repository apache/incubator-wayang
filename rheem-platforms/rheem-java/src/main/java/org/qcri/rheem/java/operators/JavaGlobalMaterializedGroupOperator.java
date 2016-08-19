package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.GlobalMaterializedGroupOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.*;

/**
 * Java implementation of the {@link GlobalMaterializedGroupOperator}.
 */
public class JavaGlobalMaterializedGroupOperator<Type>
        extends GlobalMaterializedGroupOperator<Type>
        implements JavaExecutionOperator {

    public JavaGlobalMaterializedGroupOperator(DataSetType<Type> inputType, DataSetType<Iterable<Type>> outputType) {
        super(inputType, outputType);
    }

    public JavaGlobalMaterializedGroupOperator(Class<Type> typeClass) {
        super(typeClass);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaGlobalMaterializedGroupOperator(GlobalMaterializedGroupOperator<Type> that) {
        super(that);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs,
                         ChannelInstance[] outputs,
                         JavaExecutor javaExecutor,
                         OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == 1;
        assert outputs.length == 1;

        CollectionChannel.Instance inputChannelInstance = (CollectionChannel.Instance) inputs[0];
        final Collection<?> dataQuanta = inputChannelInstance.provideCollection();
        Collection<Iterable<?>> dataQuantaGroup = new ArrayList<>(1);
        dataQuantaGroup.add(dataQuanta);

        CollectionChannel.Instance outputChannelInstance = (CollectionChannel.Instance) outputs[0];
        outputChannelInstance.accept(dataQuantaGroup);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }


    @Override
    public Optional<LoadProfileEstimator<ExecutionOperator>> createLoadProfileEstimator(Configuration configuration) {
        final NestableLoadProfileEstimator<ExecutionOperator> estimator = LoadProfileEstimators.createFromJuelSpecification(
                configuration.getStringProperty("rheem.java.globalgroup.load")
        );
        return Optional.of(estimator);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaGlobalMaterializedGroupOperator<>(this.getInputType(), this.getOutputType());
    }

    @Override
    public boolean isExecutedEagerly() {
        return true;
    }
}
