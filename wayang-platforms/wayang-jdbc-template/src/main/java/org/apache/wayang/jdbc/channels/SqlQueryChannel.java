/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.jdbc.channels;

import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.AbstractChannelInstance;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;

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

        private String jdbcName = null;

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

        public void setJdbcName(String jdbcName) {this.jdbcName = jdbcName;}

        public String getJdbcName(){ return this.jdbcName;}
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
