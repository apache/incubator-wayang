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

package org.apache.wayang.core.plan.executionplan;

import org.apache.commons.lang3.Validate;
import org.apache.wayang.core.platform.Platform;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Complete data flow on a single platform, that consists of multiple {@link ExecutionStage}s.
 */
public class PlatformExecution {

    private static final AtomicInteger SEQUENCE_NUMBER_GENERATOR = new AtomicInteger(0);

    private final int sequenceNumber;

    private Collection<ExecutionStage> stages = new LinkedList<>();

    private final Platform platform;

    public PlatformExecution(Platform platform) {
        this.platform = platform;
        this.sequenceNumber = SEQUENCE_NUMBER_GENERATOR.getAndIncrement();
    }

    void addStage(ExecutionStage stage) {
        Validate.isTrue(stage.getPlatformExecution() == this);
        this.stages.add(stage);
    }

    public Collection<ExecutionStage> getStages() {
        return this.stages;
    }

    public Platform getPlatform() {
        return this.platform;
    }

    public ExecutionStage createStage(ExecutionStageLoop executionStageLoop, int sequenceNumber) {
        return new ExecutionStage(this, executionStageLoop, sequenceNumber);
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.platform);
    }

    int getSequenceNumber() {
        return this.sequenceNumber;
    }

    public void retain(Set<ExecutionStage> retainableStages) {
        this.stages.retainAll(retainableStages);
    }
}
