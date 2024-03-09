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

package org.apache.wayang.ml.encoding;

import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.optimizer.enumeration.ExecutionTaskFlow;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.optimizer.enumeration.StageAssignmentTraversal;

import java.util.Collection;
import java.util.Collections;

public class TreeDecoder {

    /**
     * <i>Currently not used.</i>
     */
    private static final StageAssignmentTraversal.StageSplittingCriterion stageSplittingCriterion =
            (producerTask, channel, consumerTask) -> false;

    public static ExecutionPlan decode(String encoded) {
        TreeNode node = TreeNode.fromString(encoded);
        System.out.println(node);

        final ExecutionTaskFlow executionTaskFlow = new ExecutionTaskFlow(reconstructSinkTasks(node));
        final ExecutionPlan executionPlan = ExecutionPlan.createFrom(executionTaskFlow, stageSplittingCriterion);

        return executionPlan;
    }

    public static Collection<ExecutionTask> reconstructSinkTasks(TreeNode node) {
        return Collections.emptySet();
    }
}
