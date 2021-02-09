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
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionStageLoop;

/**
 * This {@link Breakpoint} implementation always requests a break unless inside of {@link ExecutionStageLoop}s.
 */
public class NoIterationBreakpoint implements Breakpoint {

    @Override
    public boolean permitsExecutionOf(ExecutionStage stage, ExecutionState state, OptimizationContext context) {
        // TODO: We could break, if we enter a loop, however, multi-stage loop heads have feedback predecessors.
        return stage.getLoop() != null && stage.getPredecessors().stream().anyMatch(
                predecessor -> predecessor.getLoop() != null
        );
    }

}
