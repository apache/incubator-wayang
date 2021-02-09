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
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.lineage.ChannelLineageNode;

import java.util.OptionalLong;

/**
 * Represents the actual, allocated resource represented by {@link Channel}.
 */
public interface ChannelInstance extends ExecutionResource {

    /**
     * @return the {@link Channel} that is implemented by this instance
     */
    Channel getChannel();

    /**
     * Optionally provides the measured cardinality of this instance. However, such a cardinality might not be available
     * for several reasons. For instance, the measurement might not have been requested or could not be implemented
     * by the executing {@link Platform}.
     *
     * @return the measured cardinality if available
     */
    OptionalLong getMeasuredCardinality();

    /**
     * Register the measured cardinality with this instance.
     */
    void setMeasuredCardinality(long cardinality);

    /**
     * Tells whether this instance should be instrumented
     */
    default boolean isMarkedForInstrumentation() {
        return this.getChannel().isMarkedForInstrumentation();
    }

    /**
     * Provides a {@link ChannelLineageNode} that keeps around (at least) all non-executed predecessor
     * {@link ChannelInstance}s and {@link org.apache.wayang.core.optimizer.OptimizationContext.OperatorContext}s.
     *
     * @return the {@link ChannelLineageNode}
     */
    ChannelLineageNode getLineage();

    /**
     * Tells whether this instance was already produced.
     *
     * @return whether this instance was already produced
     */
    boolean wasProduced();

    /**
     * Mark this instance as produced.
     */
    void markProduced();

    /**
     * Retrieve the {@link OptimizationContext.OperatorContext} of the {@link ExecutionOperator} producing this instance.
     *
     * @return the {@link OptimizationContext.OperatorContext}
     */
    OptimizationContext.OperatorContext getProducerOperatorContext();

}
