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
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;

public class TreeEncoder implements Encoder {
    public static Node encode(PlanImplementation plan) {
        List<Node> result = new ArrayList<TreeEncoder.Node>();

        HashMap<Operator, Collection<Operator>> tree = new HashMap<>();
        Collection<Operator> sinks = plan.getOperators().stream()
                .filter(op -> op.isSink())
                .collect(Collectors.toList());

        for (Operator sink : sinks) {
            Node sinkNode = traverse(sink, tree);
            sinkNode.isRoot = true;
            result.add(sinkNode);
        }

        if (result.size() == 0) {
            return null;
        }

        System.out.println("PlanImplementation");
        System.out.println(result.get(0));

        return result.get(0);
    }

    public static Node encode(WayangPlan plan, WayangContext context) {
        List<Node> result = new ArrayList<TreeEncoder.Node>();
        plan.prune();

        HashMap<Operator, Collection<Operator>> tree = new HashMap<>();
        Collection<Operator> sinks = plan.getSinks();

        for (Operator sink : sinks) {
            Node sinkNode = traverse(sink, tree);
            sinkNode.isRoot = true;
            result.add(sinkNode);
        }

        if (result.size() == 0) {
            return null;
        }

        System.out.println("WayangPlan");
        System.out.println(result.get(0));

        return result.get(0);
    }

    public static Node encode(ExecutionPlan plan) {
        List<Node> result = new ArrayList<TreeEncoder.Node>();
        HashMap<Operator, Collection<ExecutionTask>> tree = new HashMap<>();
        Set<ExecutionTask> tasks = plan.collectAllTasks();

        Collection<ExecutionTask> sinks = tasks.stream()
            .filter(task -> task.getOperator().isSink())
            .collect(Collectors.toList());

        for (ExecutionTask sink : sinks) {
            Node sinkNode = traverse(sink, tree);
            sinkNode.isRoot = true;
            result.add(sinkNode);
        }

        if (result.size() == 0) {
            return null;
        }

        System.out.println("ExecutionPlan");
        System.out.println(result.get(0));

        return result.get(0);
    }

    private static Node traverse(Operator current, HashMap<Operator, Collection<Operator>> visited) {
        if (visited.containsKey(current)) {
            return null;
        }

        Collection<Operator> inputs = Stream.of(current.getAllInputs())
            .filter(input -> input.getOccupant() != null)
            .map(input -> input.getOccupant().getOwner())
            .collect(Collectors.toList());

        /*
        Collection<Operator> outputs = Stream.of(current.getAllOutputs())
            .flatMap(output -> {
                return output
                    .getOccupiedSlots()
                    .stream()
                    .map(input -> input.getOwner());
            })
            .collect(Collectors.toList());*/

        Node currentNode = new Node();
        if (current.isExecutionOperator()) {
            currentNode.encoded = OneHotEncoder.encodeOperator((ExecutionOperator) current);
        } else {
            currentNode.encoded = OneHotEncoder.encodeOperator(current);
        }

        for (Operator input : inputs) {
            Node next = traverse(input, visited);

            if (currentNode.left == null) {
                currentNode.left = next;
            } else {
                currentNode.right = next;
            }
        }

        return currentNode;
    }

    private static Node traverse(ExecutionTask current, HashMap<Operator, Collection<ExecutionTask>> visited) {
        if (visited.containsKey(current)) {
            return null;
        }

        Collection<ExecutionTask> producers = Stream.of(current.getInputChannels())
            .map(channel -> channel.getProducer())
            .collect(Collectors.toList());

        Node currentNode = new Node();
        currentNode.encoded = OneHotEncoder.encodeOperator(current.getOperator());

        for (ExecutionTask producer : producers) {
            Node next = traverse(producer, visited);

            if (currentNode.left == null) {
                currentNode.left = next;
            } else {
                currentNode.right = next;
            }
        }

        return currentNode;
    }

    public static class Node {
        public long[] encoded;
        public Node left;
        public Node right;
        public boolean isRoot;

        @Override
        public String toString() {
            String encodedString = Arrays.toString(encoded).replace("[", "(").replace("]", ")");

            return "(" +
              encodedString +
              ',' + (left != null ? left.toString() : Arrays.toString(OneHotEncoder.encodeNullOperator()).replace("[", "(").replace("]", ")")) + ',' +
              (right != null ? right.toString() : Arrays.toString(OneHotEncoder.encodeNullOperator()).replace("[", "(").replace("]", ")")) +
              ')';
        }
    }
}
