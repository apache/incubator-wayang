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

package org.apache.wayang.core.profiling;


import org.apache.wayang.commons.util.profiledb.model.Measurement;
import org.apache.wayang.commons.util.profiledb.model.Type;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.costs.TimeEstimate;
import org.apache.wayang.core.platform.PartialExecution;

/**
 * This {@link Measurement} adapts a {@link PartialExecutionMeasurement}.
 */
@Type("partial-execution")
public class PartialExecutionMeasurement extends Measurement {

    /**
     * @see PartialExecution#getMeasuredExecutionTime()
     */
    private long executionMillis;

    /**
     * @see PartialExecution#getOverallTimeEstimate(Configuration)
     */
    private TimeEstimate estimatedExecutionMillis;


    /**
     * Serialization constructor.
     */
    private PartialExecutionMeasurement() {
    }

    /**
     * Creates a new instance.
     *
     * @param id               the ID of the new instance
     * @param partialExecution provides data for the new instance
     * @param configuration    required to calculate the estimated execution time
     */
    public PartialExecutionMeasurement(String id, PartialExecution partialExecution, Configuration configuration) {
        super(id);
        // TODO: Capture what has been executed?
        this.executionMillis = partialExecution.getMeasuredExecutionTime();
        this.estimatedExecutionMillis = partialExecution.getOverallTimeEstimate(configuration);
    }

    public long getExecutionMillis() {
        return executionMillis;
    }

    public void setExecutionMillis(long executionMillis) {
        this.executionMillis = executionMillis;
    }

    public TimeEstimate getEstimatedExecutionMillis() {
        return estimatedExecutionMillis;
    }

    public void setEstimatedExecutionMillis(TimeEstimate estimatedExecutionMillis) {
        this.estimatedExecutionMillis = estimatedExecutionMillis;
    }
}
