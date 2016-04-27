package org.qcri.rheem.spark.channels;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

/**
 * {@link ChannelInitializer} implementation to make use of {@link CollectionChannel}s in the {@link SparkPlatform}.
 */
public class CollectionChannelInitializer implements SparkChannelInitializer {

    @Override
    public Tuple<Channel, Channel> setUpOutput(ChannelDescriptor descriptor, OutputSlot<?> outputSlot, OptimizationContext optimizationContext) {
        assert outputSlot.getOwner() instanceof SparkExecutionOperator;

        return null;
    }

    @Override
    public Channel setUpOutput(ChannelDescriptor descriptor, Channel source, OptimizationContext optimizationContext) {
        return null;
    }

    @Override
    public RddChannel provideRddChannel(Channel channel, OptimizationContext optimizationContext) {
        return null;
    }
}
