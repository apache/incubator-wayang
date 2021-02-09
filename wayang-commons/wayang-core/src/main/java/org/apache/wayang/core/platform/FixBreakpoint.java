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
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Describes when to interrupt the execution of an {@link ExecutionPlan}.
 */
public class FixBreakpoint implements Breakpoint {

    /**
     * {@link ExecutionStage}s that should not yet be executed.
     */
    private final Set<ExecutionStage> stagesToSuspend = new HashSet<>();

    public FixBreakpoint breakAfter(ExecutionStage stage) {
        return this.breakBefore(stage.getSuccessors());
    }

    public FixBreakpoint breakBefore(ExecutionStage stage) {
        return this.breakBefore(Collections.singleton(stage));
    }

    public FixBreakpoint breakBefore(Collection<ExecutionStage> stages) {
        for (ExecutionStage stage : stages) {
            if (this.stagesToSuspend.add(stage)) {
                this.breakBefore(stage.getSuccessors());
            }
        }
        return this;
    }

    @Override
    public boolean permitsExecutionOf(ExecutionStage stage,
                                      ExecutionState state,
                                      OptimizationContext optimizationContext) {
        return !this.stagesToSuspend.contains(stage);
    }

}
