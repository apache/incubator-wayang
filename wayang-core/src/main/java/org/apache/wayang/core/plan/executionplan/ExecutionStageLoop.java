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

import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.LoopHeadOperator;
import org.apache.wayang.core.plan.wayangplan.LoopSubplan;

import java.util.Collection;
import java.util.HashSet;

/**
 * This class models the execution equivalent of {@link LoopSubplan}s.
 */
public class ExecutionStageLoop {

    private final LoopSubplan loopSubplan;

    private ExecutionStage headStageCache;

    private final Collection<ExecutionStage> allStages = new HashSet<>();

    public ExecutionStageLoop(LoopSubplan loopSubplan) {
        this.loopSubplan = loopSubplan;
    }

    public void add(ExecutionStage executionStage) {
        if (this.allStages.add(executionStage)) {
            if (this.headStageCache == null && this.checkForLoopHead(executionStage)) {
                this.headStageCache = executionStage;
            }
        }
    }

    private boolean checkForLoopHead(ExecutionStage executionStage) {
        return executionStage.getAllTasks().stream().anyMatch(this::isLoopHead);
    }

    /**
     * Checks whether the given {@link ExecutionTask} is the {@link LoopHeadOperator} of the {@link #loopSubplan}.
     *
     * @param task to be checked
     * @return whether it is the {@link LoopHeadOperator}
     */
    private boolean isLoopHead(ExecutionTask task) {
        final ExecutionOperator operator = task.getOperator();
        return operator.isLoopHead() && operator.getInnermostLoop() == this.loopSubplan;
    }

    /**
     * Inspect whether {@link ExecutionTask} is the {@link LoopHeadOperator} of the {@link #loopSubplan}. If so,
     * promote its {@link ExecutionStage} as the loop head.
     *
     * @param task to be checked
     */
    public void update(ExecutionTask task) {
        if (this.headStageCache == null && this.isLoopHead(task)) {
            this.headStageCache = task.getStage();
        }
    }

    /**
     * Retrieves the {@link ExecutionStage} that contains the {@link LoopHeadOperator} of this instance.
     *
     * @return the loop head {@link ExecutionStage}
     */
    public ExecutionStage getLoopHead() {
        if (this.headStageCache == null) {
            this.allStages.stream()
                    .flatMap(executionStage -> executionStage.getAllTasks().stream())
                    .forEach(this::update);
        }
        assert this.headStageCache != null : String.format("No ExecutionStageLoop head for %s.", this.loopSubplan);

        return this.headStageCache;
    }

    /**
     * Retrieve the {@link LoopSubplan} encapsulated by this instance.
     *
     * @return the {@link LoopSubplan}
     */
    public LoopSubplan getLoopSubplan() {
        return loopSubplan;
    }
}
