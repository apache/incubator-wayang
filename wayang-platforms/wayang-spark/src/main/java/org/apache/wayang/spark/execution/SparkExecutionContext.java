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

package org.apache.wayang.spark.execution;

import org.apache.spark.broadcast.Broadcast;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.function.ExecutionContext;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.spark.channels.BroadcastChannel;
import org.apache.wayang.spark.operators.SparkExecutionOperator;
import org.apache.wayang.spark.platform.SparkPlatform;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link ExecutionContext} implementation for the {@link SparkPlatform}.
 */
public class SparkExecutionContext implements ExecutionContext, Serializable {

    /**
     * Iteration number of the execution.
     */
    private int iterationNumber;

    /**
     * Mapping of broadcast name to {@link Broadcast} references.
     */
    private Map<String, Broadcast<?>> broadcasts;

    /**
     * Creates a new instance.
     *
     * @param operator {@link SparkExecutionOperator} for that the instance should be created
     * @param inputs   {@link ChannelInstance} inputs for the {@code operator}
     */
    public SparkExecutionContext(SparkExecutionOperator operator, ChannelInstance[] inputs, int iterationNumber) {
        this.broadcasts = new HashMap<>();
        for (int inputIndex = 0; inputIndex < operator.getNumInputs(); inputIndex++) {
            InputSlot<?> inputSlot = operator.getInput(inputIndex);
            if (inputSlot.isBroadcast()) {
                final BroadcastChannel.Instance broadcastChannelInstance = (BroadcastChannel.Instance) inputs[inputIndex];
                this.broadcasts.put(inputSlot.getName(), broadcastChannelInstance.provideBroadcast());
            }
        }
        this.iterationNumber = iterationNumber;
    }

    /**
     * Creates a new instance.
     */
    public SparkExecutionContext(int iterationNumber) {
        this.iterationNumber = iterationNumber;
    }

    /**
     * For serialization purposes.
     */
    @SuppressWarnings("unused")
    private SparkExecutionContext() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Collection<T> getBroadcast(String name) {
        final Broadcast<?> broadcast = this.broadcasts.get(name);
        if (broadcast == null) {
            throw new WayangException("No such broadcast found: " + name);
        }

        return (Collection<T>) broadcast.getValue();
    }

    @Override
    public int getCurrentIteration() {
        return this.iterationNumber;
    }
}
