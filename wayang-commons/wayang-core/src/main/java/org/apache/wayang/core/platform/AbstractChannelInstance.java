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

package org.apache.wayang.core.platform;

import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.lineage.ChannelLineageNode;
import org.apache.logging.log4j.LogManager;

import java.util.OptionalLong;

/**
 * Template for {@link ChannelInstance} implementations.
 */
public abstract class AbstractChannelInstance extends ExecutionResourceTemplate implements ChannelInstance {

    private OptionalLong measuredCardinality = OptionalLong.empty();

    private boolean wasProduced = false;

    /**
     * The {@link OptimizationContext.OperatorContext} of the {@link ExecutionOperator} that is producing this
     * instance.
     */
    private final OptimizationContext.OperatorContext producerOperatorContext;

    private ChannelLineageNode lineage;

    /**
     * Creates a new instance and registers it with its {@link Executor}.
     *
     * @param executor                that maintains this instance
     * @param producerOperatorContext the {@link OptimizationContext.OperatorContext} for the producing
     *                                {@link ExecutionOperator}
     * @param producerOutputIndex     the output index of the producer {@link ExecutionTask}
     */
    protected AbstractChannelInstance(Executor executor,
                                      OptimizationContext.OperatorContext producerOperatorContext,
                                      int producerOutputIndex) {
        super(executor);
        this.lineage = new ChannelLineageNode(this);
        this.producerOperatorContext = producerOperatorContext;
    }

    @Override
    public OptionalLong getMeasuredCardinality() {
        return this.measuredCardinality;
    }

    @Override
    public void setMeasuredCardinality(long cardinality) {
        this.measuredCardinality.ifPresent(oldCardinality -> {
            if (oldCardinality != cardinality) {
                LogManager.getLogger(this.getClass()).warn(
                        "Replacing existing measured cardinality of {} with {} (was {}).",
                        this.getChannel(),
                        cardinality,
                        oldCardinality
                );
            }
        });
        this.measuredCardinality = OptionalLong.of(cardinality);
    }

    @Override
    public ChannelLineageNode getLineage() {
        return this.lineage;
    }

    @Override
    public boolean wasProduced() {
        return this.wasProduced;
    }

    @Override
    public void markProduced() {
        this.wasProduced = true;
    }

    @Override
    public OptimizationContext.OperatorContext getProducerOperatorContext() {
        return this.producerOperatorContext;
    }

    @Override
    public String toString() {
        return "*" + this.getChannel().toString();
    }
}
