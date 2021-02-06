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

import org.apache.wayang.core.optimizer.enumeration.ExecutionTaskFlow;
import org.apache.wayang.core.plan.executionplan.Channel;

import java.util.Collection;

/**
 * Contains a state of the execution of an {@link ExecutionTaskFlow}.
 */
public interface ExecutionState {

    /**
     * Register a {@link ChannelInstance}.
     *
     * @param channelInstance that should be registered
     */
    void register(ChannelInstance channelInstance);

    /**
     * Obtain a previously registered {@link ChannelInstance}.
     *
     * @param channel the {@link Channel} of the {@link ChannelInstance}
     * @return the {@link ChannelInstance} or {@code null} if none
     */
    ChannelInstance getChannelInstance(Channel channel);

    /**
     * Registers a measured cardinality.
     *
     * @param channelInstance the {@link ChannelInstance} for that there is a measured cardinality
     */
    void addCardinalityMeasurement(ChannelInstance channelInstance);

    /**
     * Retrieve previously registered cardinality measurements
     *
     * @return {@link ChannelInstance}s that contain cardinality measurements
     */
    Collection<ChannelInstance> getCardinalityMeasurements();


    /**
     * Stores a {@link PartialExecution}.
     *
     * @param partialExecution to be stored
     */
    void add(PartialExecution partialExecution);

    /**
     * Retrieves previously stored {@link PartialExecution}s.
     *
     * @return the {@link PartialExecution}s
     */
    Collection<PartialExecution> getPartialExecutions();
}
