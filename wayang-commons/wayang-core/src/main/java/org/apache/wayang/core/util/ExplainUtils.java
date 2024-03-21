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

import org.apache.commons.lang.StringUtils;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.executionplan.Channel;

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

    public static void parsePlan(WayangPlan plan, boolean upstream) {
        System.out.println("== Wayang Plan ==");
        HashMap<Operator, Collection<Operator>> tree = new HashMap<>();
        Collection<Operator> roots = plan.collectReachableTopLevelSources();

        if (upstream) {
            roots = plan.getSinks();
        }

        for (Operator root : roots) {
            traverse(root, upstream, tree, 0);
        }
    }

    public static void parsePlan(ExecutionPlan plan, boolean upstream) {
        System.out.println("== Execution Plan ==");
        HashMap<Operator, Collection<ExecutionTask>> tree = new HashMap<>();
        Set<ExecutionTask> tasks = plan.collectAllTasks();

        Collection<ExecutionTask> roots;

        if (upstream) {
            roots = tasks.stream()
            .filter(task -> task.getOperator().isSink())
            .collect(Collectors.toList());
        } else {
            roots = tasks.stream()
            .filter(task -> task.getOperator().isSource())
            .collect(Collectors.toList());
        }

        for (ExecutionTask root : roots) {
            traverse(root, upstream, tree, 0);
        }
    }

    private static void traverse(Operator current, boolean upstream, HashMap<Operator, Collection<Operator>> visited, int level) {
        if (current instanceof OperatorAlternative) {
            OperatorAlternative alts = (OperatorAlternative) current;
            System.out.println(StringUtils.repeat(ExplainUtils.INDENT, level) + "-+ " + alts.getAlternatives());
        } else {
            System.out.println(StringUtils.repeat(ExplainUtils.INDENT, level) + "-+ " + current);
        }

        if (visited.containsKey(current)) {
            return;
        }

        Collection<Operator> children;

        if (upstream) {
            children = Stream.of(current.getAllInputs())
                .filter(input -> input.getOccupant() != null)
                .map(input -> input.getOccupant().getOwner())
                .collect(Collectors.toList());
        } else {
            children = Stream.of(current.getAllOutputs())
                .flatMap(output -> {
                    return output
                        .getOccupiedSlots()
                        .stream()
                        .map(input -> input.getOwner());
                })
                .collect(Collectors.toList());
        }

        for (Operator child : children) {
            traverse(child, upstream, visited, level + 1);
        }
    }

    private static void traverse(ExecutionTask current, boolean upstream, HashMap<Operator, Collection<ExecutionTask>> visited, int level) {
        System.out.println(StringUtils.repeat(ExplainUtils.INDENT, level) + "-+ " + current.getOperator());

        if (visited.containsKey(current)) {
            return;
        }

        Collection<ExecutionTask> children;

        if (upstream) {
            children = Stream.of(current.getInputChannels())
                .map(Channel::getProducer).collect(Collectors.toList());
        } else {
            children = Stream.of(current.getOutputChannels())
                .flatMap(output -> output.getConsumers().stream())
                .collect(Collectors.toList());
        }

        for (ExecutionTask child : children) {
            traverse(child, upstream, visited, level + 1);
        }
    }
}
