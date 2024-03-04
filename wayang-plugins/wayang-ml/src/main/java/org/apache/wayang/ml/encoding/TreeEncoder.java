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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;

public class TreeEncoder implements Encoder {
    public static long[] encode(PlanImplementation plan) {
        return new long[0];
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

        System.out.println(result.get(0));

        return result.get(0);
    }

    private static Node traverse(Operator current, HashMap<Operator, Collection<Operator>> visited) {
        if (visited.containsKey(current)) {
            return null;
        }

        Collection<Operator> inputs = Stream.of(current.getAllInputs())
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

        visited.put(current, inputs);

        Node currentNode = new Node();
        currentNode.encoded = OneHotEncoder.encodeOperator(current);

        for (Operator output : inputs) {
            Node next = traverse(output, visited);

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
            return "TreeNode{" +
            "encoded=" + Arrays.toString(encoded) +
            ", left=" + (left != null ? left.toString() : null) +
            ", right=" + (right != null ? right.toString() : null) +
            ", isRoot=" + isRoot +
            '}';
        }
    }
}
