package org.qcri.rheem.jdbc.channels;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.jdbc.JdbcPlatformTemplate;

import java.util.Objects;

/**
 * Represents connections between {@link ExecutionOperator}s that are bundled in a single database query.
 */
public class InternalDatabaseChannel extends Channel {

    public InternalDatabaseChannel(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
    }

    private InternalDatabaseChannel(InternalDatabaseChannel parent) {
        super(parent);
    }

    @Override
    public InternalDatabaseChannel copy() {
        return new InternalDatabaseChannel(this);
    }

    @Override
    public ChannelInstance createInstance(Executor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
        throw new UnsupportedOperationException(String.format("%s cannot be instantiated.", this));
    }

    /**
     * Describes a specific class of {@link InternalDatabaseChannel}s belonging to a certain {@link JdbcPlatformTemplate}.
     */
    public static class Descriptor extends ChannelDescriptor {

        private static final boolean IS_REUSABLE = true;

        private static final boolean IS_INTERNAL = true;

        /**
         * {@link Platform} to which corresponding {@link Channel}s belong.
         */
        private final JdbcPlatformTemplate platform;

        public Descriptor(JdbcPlatformTemplate platform) {
            super(InternalDatabaseChannel.class, IS_REUSABLE, IS_REUSABLE, IS_REUSABLE & !IS_INTERNAL);
            this.platform = platform;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Descriptor that = (Descriptor) o;
            return Objects.equals(platform, that.platform);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), platform);
        }
    }
}