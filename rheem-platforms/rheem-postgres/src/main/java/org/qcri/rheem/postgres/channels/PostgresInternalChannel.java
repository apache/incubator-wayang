package org.qcri.rheem.postgres.channels;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.util.Tuple;

/**
 * Dummy channel to connect postgres operators.
 */
public class PostgresInternalChannel extends Channel {

    private static final boolean IS_REUSABLE = true;

    private static final boolean IS_INTERNAL = true;

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(PostgresInternalChannel.class,
            IS_REUSABLE, IS_REUSABLE, IS_REUSABLE & !IS_INTERNAL);

    public PostgresInternalChannel(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
        assert descriptor == DESCRIPTOR;
    }

    private PostgresInternalChannel(PostgresInternalChannel parent) {
        super(parent);
    }

    @Override
    public PostgresInternalChannel copy() {
        return new PostgresInternalChannel(this);
    }

    public static class Initializer implements ChannelInitializer {

        @Override
        public Tuple<Channel, Channel> setUpOutput(ChannelDescriptor descriptor, OutputSlot<?> outputSlot,
                                                   OptimizationContext optimizationContext) {
            PostgresInternalChannel channel = new PostgresInternalChannel(descriptor, outputSlot);
            return new Tuple<>(channel, channel);

        }

        @Override
        public Channel setUpOutput(ChannelDescriptor descriptor, Channel source,
                                   OptimizationContext optimizationContext) {
            return source;
        }
    }
}