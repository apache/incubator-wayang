package org.qcri.rheem.spark.operators;

import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.OperatorBase;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Converts a {@link RddChannel} into a {@link CollectionChannel} of the {@link JavaPlatform}.
 */
public class SparkCollectOperator<Type>
        extends OperatorBase
        implements SparkExecutionOperator {

    public SparkCollectOperator(DataSetType<Type> type) {
        super(1, 1, false, null);
        this.inputSlots[0] = new InputSlot<>("input", this, type);
        this.outputSlots[0] = new OutputSlot<>("output", this, type);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        CollectionChannel.Instance output = (CollectionChannel.Instance) outputs[0];

        @SuppressWarnings("unchecked")
        final List<Type> collectedRdd = (List<Type>) input.provideRdd().collect();
        output.accept(collectedRdd);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }
}
