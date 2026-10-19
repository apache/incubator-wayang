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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.wayang.core.plan.wayangplan.Operator;

public class TreeNode {
    private static final int PADDING_SIZE = 1;

    private static final Pattern pattern = Pattern.compile(
            "\\(\\((?<value>[+,-]?\\d+(?:,\\s*\\d+)*)\\),(?<children>(?<left>\\s*\\(.+\\)),(?<right>\\s*\\(.+\\))|\\)*)",
            Pattern.CASE_INSENSITIVE);

    public static TreeNode fromString(final String encoded) {
        final TreeNode result = new TreeNode();
        final Matcher matcher = pattern.matcher(encoded);

        if (!matcher.find()) {
            return null;
        }

        final String value = matcher.group("value");
        final String left = matcher.group("left");
        final String right = matcher.group("right");
        final long[] encodedLongs = Arrays.stream(value.split(","))
            .map(String::trim)
            .mapToLong(Long::parseLong)
            .toArray();

        // ignore if no platform choices given
        if (Arrays.stream(encodedLongs).sum() == 0L) {
            return null;
        }

        result.encoded = encodedLongs;

        if (left != null) {
            result.left = TreeNode.fromString(left);
        }

        if (right != null) {
            result.right = TreeNode.fromString(right);
        }

        return result;
    }

    public static TreeNode create() {
        return new TreeNode();
    }

    public long[] encoded;

    public TreeNode left;

    public TreeNode right;

    public Operator operator;

    public TreeNode() {
        this.operator = null;
        this.encoded = OneHotEncoder.encodeNullOperator();
        this.left = null;
        this.right = null;
    }

    public TreeNode(final long[] encoded, final TreeNode left, final TreeNode right) {
        this.operator = null;
        this.encoded = encoded;
        this.left = left;
        this.right = right;
    }

    public TreeNode(final Operator operator, final long[] encoded, final TreeNode left, final TreeNode right) {
        this.operator = operator;
        this.encoded = encoded;
        this.left = left;
        this.right = right;
    }

    /*
     * Utility function to rebalance the tree to a guaranteed BinaryTree
     *
     * @return void
     */
    public void rebalance() {
        if (this.isLeaf()) {
            this.left = TreeNode.create();
            this.right = TreeNode.create();
            return;
        }

        if (this.left != null) {
            this.left.rebalance();
        }

        if (this.right != null) {
            this.right.rebalance();
        }

        if (this.left == null && this.right != null) {
            this.left = TreeNode.create();
        }

        if (this.left != null && this.right == null) {
            this.right = TreeNode.create();
        }
    }

    public TreeNode getLeft() {
        return this.left;
    }

    public TreeNode getRight() {
        return this.right;
    }

    public String display() {
        return Long.toString(this.encoded[0]);
    }

    public String toStringEncoding() {
        final String encodedString = Arrays.toString(encoded).replace("[", "(").replace("]", ")").replaceAll("\\s+",
                "");

        if (this.getLeft() == null && this.getRight() == null) {
            return '(' + encodedString + ",)";
        }

        String leftString = "";

        if (this.getLeft() != null) {
            final TreeNode castLeft = this.getLeft();

            if (castLeft.isNullOperator()) {
                leftString = Arrays.toString(OneHotEncoder.encodeNullOperator()).replace("[", "((").replace("]", "),)")
                        .replaceAll("\\s+", "");
            } else {
                leftString = castLeft.toStringEncoding();
            }
        }

        String rightString = "";
        
        if (this.getRight() != null) {
            final TreeNode castRight = this.getRight();

            if (castRight.isNullOperator()) {
                rightString = Arrays.toString(OneHotEncoder.encodeNullOperator()).replace("[", "((").replace("]", "),)")
                        .replaceAll("\\s+", "");
            } else {
                rightString = castRight.toStringEncoding();
            }
        }

        return "(" + encodedString + "," + leftString + "," + rightString + ")";
    }

    public TreeNode withIdsFrom(final TreeNode node) {
        this.encoded[0] = node.encoded[0];

        if (this.getLeft() != null && node.getLeft() != null) {
            this.left = this.getLeft().withIdsFrom(node.getLeft());
        }

        if (this.getRight() != null && node.getRight() != null) {
            this.right = this.getRight().withIdsFrom(node.getRight());
        }

        return this;
    }

    public TreeNode withPlatformChoicesFrom(final TreeNode node) {
        if (this.isNullOperator()) {
            return this;
        }

        if (this.encoded == OneHotEncoder.encodeNullOperator()) {
            return this;
        }

        if (node.encoded == null) {
            assert this.encoded != null;
            return this;
        }
        final HashMap<String, Integer> platformMappings = OneHotMappings.getPlatformsMapping();
        final HashMap<String, Integer> operatorMappings = OneHotMappings.getOperatorMapping();
        final int operatorsCount = operatorMappings.size();
        final int platformsCount = platformMappings.size();

        if (this.encoded.length > 0) {
            // Check if this already encodes a platform specific operator
            final long[] platformChoices = Arrays.copyOfRange(this.encoded, PADDING_SIZE + operatorsCount,
                    PADDING_SIZE + operatorsCount + platformsCount);

            if (ArrayUtils.indexOf(platformChoices, 1) != -1) {
                return this;
            }
        }

        int platformPosition = -1;
        platformPosition = ArrayUtils.indexOf(node.encoded, 1);
        String platform = "";

        assert platformPosition >= 0;

        for (final Map.Entry<String, Integer> pair : platformMappings.entrySet()) {
            if (pair.getValue() == platformPosition) {
                platform = pair.getKey();
            }
        }

        assert platform != "";

        this.encoded[PADDING_SIZE + operatorsCount + platformPosition] = 1;

        if (this.getLeft() != null && node.getLeft() != null) {
            this.left = this.getLeft().withPlatformChoicesFrom(node.getLeft());
        }

        if (this.getRight() != null && node.getRight() != null) {
            this.right = this.getRight().withPlatformChoicesFrom(node.getRight());
        }

        return this;
    }

    public void softmax() {
        if (this.encoded == null || this.encoded == OneHotEncoder.encodeNullOperator()) {
            return;
        }

        // All set to 1, aka null operator
        if (Arrays.stream(this.encoded).sum() == 9) {
            return;
        }

        final long maxValue = Arrays.stream(this.encoded).max().getAsLong();
        final long[] values = Arrays.stream(this.encoded).map(value -> value == maxValue ? 1 : 0).toArray();

        this.encoded = values;

        if (this.getLeft() != null) {
            this.getLeft().softmax();
        }

        if (this.getRight() != null) {
            this.getRight().softmax();
        }
    }

    public boolean isNullOperator() {
        return this.operator == null && Arrays.equals(this.encoded, OneHotEncoder.encodeNullOperator());
    }

    /*
     * Utility function for tree traversal without return value. Can be used for
     * mutation.
     *
     * @param Consumer<BinaryTree<T>> func
     * 
     * @return void
     */
    public void traverse(final Consumer<TreeNode> func) {
        func.accept(this);

        if (this.isLeaf()) {
            return;
        }

        if (this.left != null) {
            this.left.traverse(func);
        }

        if (this.right != null) {
            this.right.traverse(func);
        }
    }

    public boolean isLeaf() {
        return this.left == null && this.right == null;
    }

    public TreeNode getNode(final int index) {
        final List<TreeNode> nodes = new ArrayList<>();

        nodes.add(new TreeNode());

        this.traverse(nodes::add);

        return nodes.get(index);
    }

    public int getNumberOfNodes() {
        final List<TreeNode> nodes = new ArrayList<>();

        // Add null operator
        nodes.add(new TreeNode());

        this.traverse(nodes::add);

        return nodes.size();
    }

    public int size() {
        int size = 1;

        if (this.isLeaf()) {
            return 1;
        }

        if (left != null) {
            size += this.left.size();
        }

        if (right != null) {
            size += this.right.size();
        }

        return size;
    }
}
