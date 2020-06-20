package org.qcri.rheem.jdbc.channels;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.AbstractChannelInstance;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.jdbc.platform.JdbcPlatformTemplate;

import java.util.Objects;

/**
 * Implementation of a {@link Channel} that is given by a SQL query.
 */
public class SqlQueryChannel extends Channel {

    public SqlQueryChannel(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
    }

    private SqlQueryChannel(SqlQueryChannel parent) {
        super(parent);
    }

    @Override
    public SqlQueryChannel copy() {
        return new SqlQueryChannel(this);
    }

    @Override
    public SqlQueryChannel.Instance createInstance(Executor executor,
                                                   OptimizationContext.OperatorContext producerOperatorContext,
                                                   int producerOutputIndex) {
        return new Instance(executor, producerOperatorContext, producerOutputIndex);
    }

    /**
     * {@link ChannelInstance} implementation for {@link SqlQueryChannel}s.
     */
    public class Instance extends AbstractChannelInstance {

        private String sqlQuery = null;

        /**
         * Creates a new instance and registers it with its {@link Executor}.
         *
         * @param executor                that maintains this instance
         * @param producerOperatorContext the {@link OptimizationContext.OperatorContext} for the producing
         *                                {@link ExecutionOperator}
         * @param producerOutputIndex     the output index of the producer {@link ExecutionTask}
         */
        protected Instance(Executor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }

        @Override
        public SqlQueryChannel getChannel() {
            return SqlQueryChannel.this;
        }

        @Override
        protected void doDispose() throws Throwable {
            // Nothing to do.
        }

        public void setSqlQuery(String sqlQuery) {
            this.sqlQuery = sqlQuery;
        }

        public String getSqlQuery() {
            return this.sqlQuery;
        }
    }

    /**
     * Describes a specific class of {@link SqlQueryChannel}s belonging to a certain {@link JdbcPlatformTemplate}.
     */
    public static class Descriptor extends ChannelDescriptor {

        /**
         * {@link Platform} to which corresponding {@link Channel}s belong.
         */
        private final JdbcPlatformTemplate platform;

        public Descriptor(JdbcPlatformTemplate platform) {
            super(SqlQueryChannel.class, false, false);
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