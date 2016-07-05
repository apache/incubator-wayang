package org.qcri.rheem.postgres.channels;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.AbstractChannelInstance;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.postgres.execution.PostgresExecutor;
import org.qcri.rheem.postgres.operators.PostgresExecutionOperator;

/**
 * Pro forma {@link Channel} to connect {@link PostgresExecutionOperator}s.
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

    @Override
    public Instance createInstance(Executor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
        return new Instance((PostgresExecutor) executor, producerOperatorContext, producerOutputIndex);
    }

    /**
     * {@link ChannelInstance} implementation for {@link PostgresInternalChannel}.
     */
    public class Instance extends AbstractChannelInstance {

        public Instance(PostgresExecutor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }

        @Override
        protected void doDispose() throws Throwable {
        }

        @Override
        public Channel getChannel() {
            return PostgresInternalChannel.this;
        }
    }
}