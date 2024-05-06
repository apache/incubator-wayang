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

import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.wayangplan.ElementaryOperator;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.Junction;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.api.exception.WayangException;

public class TreeEncoder implements Encoder {
    public static TreeNode encode(PlanImplementation plan) {
        List<TreeNode> result = new ArrayList<TreeNode>();

        OneHotMappings.setOptimizationContext(plan.getOptimizationContext());

        HashMap<Operator, Collection<Operator>> tree = new HashMap<>();
        Collection<Operator> sinks = plan.getOperators().stream()
                .filter(Operator::isSink).collect(Collectors.toList());

        Map<OutputSlot<?>, Junction> junctions = plan.getJunctions();

        for (Operator sink : sinks) {
            TreeNode sinkNode = traversePIOperator(sink, junctions, tree);
            sinkNode.isRoot = true;
            result.add(sinkNode);
        }

        if (result.size() == 0) {
            return null;
        }

        return result.get(0);
    }

    public static TreeNode encode(WayangPlan plan) {
        List<TreeNode> result = new ArrayList<TreeNode>();
        plan.prune();

        HashMap<Operator, Collection<Operator>> tree = new HashMap<>();
        Collection<Operator> sinks = plan.getSinks();

        for (Operator sink : sinks) {
            TreeNode sinkNode = traverse(sink, tree);
            sinkNode.isRoot = true;
            result.add(sinkNode);
        }

        if (result.size() == 0) {
            return null;
        }

        return result.get(0);
    }

    public static TreeNode encode(ExecutionPlan plan, boolean ignoreConversions) {
        List<TreeNode> result = new ArrayList<TreeNode>();
        HashMap<Operator, Collection<ExecutionTask>> tree = new HashMap<>();
        Set<ExecutionTask> tasks = plan.collectAllTasks();

        Collection<ExecutionTask> sinks = tasks.stream()
                .filter(task -> task.getOperator().isSink()).collect(Collectors.toList());

        for (ExecutionTask sink : sinks) {
            TreeNode sinkNode = traverse(sink, tree, ignoreConversions);
            sinkNode.isRoot = true;
            result.add(sinkNode);
        }

        if (result.size() == 0) {
            return null;
        }

        return result.get(0);
    }

    private static TreeNode traversePIOperator(
            Operator current,
            Map<OutputSlot<?>, Junction> junctions,
            HashMap<Operator, Collection<Operator>> visited) {
        if (visited.containsKey(current)) {
            return null;
        }

        TreeNode currentNode = new TreeNode();

        if (current.isAlternative()) {
            Operator original = ((OperatorAlternative) current)
                .getAlternatives()
                .get(0)
                .getContainedOperators()
                .stream()
                .findFirst()
                .orElseThrow(() -> new WayangException("Operator could not be retrieved from Alternatives"));
            OneHotMappings.addOriginalOperator(original);

            currentNode.encoded = OneHotEncoder.encodeOperator(original);
        } else {
            OneHotMappings.addOriginalOperator(current);

            if (current.isExecutionOperator()) {
                currentNode.encoded = OneHotEncoder.encodeOperator((ExecutionOperator) current);
            } else {
                currentNode.encoded = OneHotEncoder.encodeOperator(current);
            }
        }

        Collection<Junction> currentJunctions = junctions.values().stream()
            .filter(junction -> {
                for (InputSlot<?> input : current.getAllInputs()) {
                    if (junction.getTargetInputs().contains(input)) {
                        return true;
                    }
                }

                return false;
            }).collect(Collectors.toList());

        Collection<ExecutionOperator> inputs = currentJunctions.stream()
            .map(junction -> junction.getSourceOperator())
            .collect(Collectors.toList());

        for (final Operator input : inputs) {

            TreeNode next;
            Collection<ExecutionTask> conversions = currentJunctions.stream()
                .filter(junction -> junction.getSourceOperator() == input)
                .flatMap(junction -> junction.getConversionTasks().stream())
                .collect(Collectors.toList());

            // Need to fit conversions in between current and its inputs
            if (conversions.size() > 0) {
                Queue<ExecutionTask> conversionQueue = new LinkedList<>();
                conversionQueue.addAll(conversions);

                next = traverseWithNext(conversionQueue, junctions, visited, input);
            } else {
                next = traversePIOperator(input, junctions, visited);
            }

            if (currentNode.left == null) {
                currentNode.left = next;
            } else {
                currentNode.right = next;
            }
        }

        return currentNode;
    }

    private static TreeNode traverseWithNext(
        Queue<ExecutionTask> conversions,
        Map<OutputSlot<?>, Junction> junctions,
        HashMap<Operator, Collection<Operator>> visited,
        Operator next
    ){
        if (visited.containsKey(next)) {
            return null;
        }

        if (conversions.isEmpty()) {
            return traversePIOperator(next, junctions, visited);
        }

        ExecutionTask currentTask = conversions.poll();
        ExecutionOperator current = currentTask.getOperator();

        TreeNode currentNode = new TreeNode();

        OneHotMappings.addOriginalOperator(current);

        if (current.isExecutionOperator()) {
            currentNode.encoded = OneHotEncoder.encodeOperator((ExecutionOperator) current);
        } else {
            currentNode.encoded = OneHotEncoder.encodeOperator(current);
        }

        TreeNode nextNode = traverseWithNext(conversions, junctions, visited, next);

        if (currentNode.left == null) {
            currentNode.left = nextNode;
        } else {
            currentNode.right = nextNode;
        }

        return currentNode;
    }

    private static TreeNode traverse(Operator current, HashMap<Operator, Collection<Operator>> visited) {
        if (visited.containsKey(current)) {
            return null;
        }

        TreeNode currentNode = new TreeNode();

        if (current.isAlternative()) {
            Operator original = ((OperatorAlternative) current)
                .getAlternatives()
                .get(0)
                .getContainedOperators()
                .stream()
                .findFirst()
                .orElseThrow(() -> new WayangException("Operator could not be retrieved from Alternatives"));
            OneHotMappings.addOriginalOperator(original);

            currentNode.encoded = OneHotEncoder.encodeOperator(original);
        } else {
            OneHotMappings.addOriginalOperator(current);

            if (current.isExecutionOperator()) {
                currentNode.encoded = OneHotEncoder.encodeOperator((ExecutionOperator) current);
            } else {
                currentNode.encoded = OneHotEncoder.encodeOperator(current);
            }
        }

        // Add for later reconstruction in TreeDecoder
        Collection<Operator> inputs = Stream.of(current.getAllInputs())
                .filter(input -> input.getOccupant() != null)
                .map(input -> input.getOccupant().getOwner()).collect(Collectors.toList());

        /*
        Collection<Operator> outputs = Stream.of(current.getAllOutputs())
            .flatMap(output -> {
                return output
                    .getOccupiedSlots()
                    .stream()
                    .map(input -> input.getOwner());
            })
            .collect(Collectors.toList());*/

        for (Operator input : inputs) {
            TreeNode next = traverse(input, visited);

            if (currentNode.left == null) {
                currentNode.left = next;
            } else {
                currentNode.right = next;
            }
        }

        return currentNode;
    }

    private static TreeNode traverse(ExecutionTask current, HashMap<Operator, Collection<ExecutionTask>> visited, boolean ignoreConversions) {
        if (visited.containsKey(current)) {
            return null;
        }

        Collection<ExecutionTask> producers = Stream.of(current.getInputChannels())
                .map(Channel::getProducer).collect(Collectors.toList());

        ExecutionOperator operator = current.getOperator();
        TreeNode currentNode = new TreeNode();
        currentNode.encoded = OneHotEncoder.encodeOperator(operator);

        for (ExecutionTask producer : producers) {
            TreeNode next = traverse(producer, visited, ignoreConversions);

            if (operator.isConversion() && ignoreConversions) {
                System.out.println("Ignored conversion: " + operator);
                return next;
            }

            if (currentNode.left == null) {
                currentNode.left = next;
            } else {
                currentNode.right = next;
            }
        }

        return currentNode;
    }

}
