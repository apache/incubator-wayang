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

package org.apache.wayang.core.util;

import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;

import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExplainUtils {

    public static final String INDENT = "  ";

    public static void parsePlan(WayangPlan plan, BiConsumer<Operator, Integer> func) {
        System.out.println("== Wayang Plan ==");
        HashMap<Operator, Collection<Operator>> tree = new HashMap<>();
        Collection<Operator> sources = plan.collectReachableTopLevelSources();

        for (Operator source : sources) {
            traverse(source, func, tree, 0);
        }
    }

    public static void parsePlan(ExecutionPlan plan, BiConsumer<ExecutionTask, Integer> func) {
        System.out.println("== Execution Plan ==");
        HashMap<Operator, Collection<ExecutionTask>> tree = new HashMap<>();
        Set<ExecutionTask> tasks = plan.collectAllTasks();

        Collection<ExecutionTask> sources = tasks.stream()
            .filter(task -> task.getOperator().isSource())
            .collect(Collectors.toList());

        for (ExecutionTask source : sources) {
            traverse(source, func, tree, 0);
        }
    }

    private static void traverse(Operator current, BiConsumer<Operator, Integer> func, HashMap<Operator, Collection<Operator>> visited, int level) {
        func.accept(current, level);

        if (visited.containsKey(current)) {
            return;
        }

        Collection<Operator> outputs = Stream.of(current.getAllOutputs())
            .flatMap(output -> {
                return output
                    .getOccupiedSlots()
                    .stream()
                    .map(input -> input.getOwner());
            })
            .collect(Collectors.toList());

        for (Operator output : outputs) {
            traverse(output, func, visited, level + 1);
        }
    }

    private static void traverse(ExecutionTask current, BiConsumer<ExecutionTask, Integer> func, HashMap<Operator, Collection<ExecutionTask>> visited, int level) {
        func.accept(current, level);

        if (visited.containsKey(current)) {
            return;
        }

        Collection<ExecutionTask> consumers = Stream.of(current.getOutputChannels())
            .flatMap(output -> output.getConsumers().stream())
            .collect(Collectors.toList());

        for (ExecutionTask consumer : consumers) {
            traverse(consumer, func, visited, level + 1);

        }
    }
}
