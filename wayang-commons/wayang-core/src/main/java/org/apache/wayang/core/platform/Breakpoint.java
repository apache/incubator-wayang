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

/**
 * Describes when to interrupt the execution of an {@link ExecutionPlan}.
 */
@FunctionalInterface
public interface Breakpoint {

    /**
     * Tests whether the given {@link ExecutionStage} can be executed.
     *
     * @param stage   whose execution is in question
     * @param state   current {@link ExecutionState}
     * @param context {@link OptimizationContext} of the last optimization process
     * @return whether the {@link ExecutionStage} should be executed
     */
    boolean permitsExecutionOf(ExecutionStage stage,
                               ExecutionState state,
                               OptimizationContext context);

    /**
     * {@link Breakpoint} implementation that never breaks.
     */
    Breakpoint NONE = (stage, state, optCtx) -> true;

}
