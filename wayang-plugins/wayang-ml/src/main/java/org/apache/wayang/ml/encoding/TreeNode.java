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

import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.util.BinaryTree;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.ArrayUtils;

public class TreeNode extends BinaryTree<long[]>{

    private static final int PADDING_SIZE = 1;

    public long[] encoded;
    //public TreeNode left;
    //public TreeNode right;
    public Operator operator = null;

    //private static Pattern pattern = Pattern.compile("\\(\\((?<value>[+,-]?\\d+(?:,\\s*\\d+)*)\\),(?<left>\\s*\\(.+\\)),(?<right>\\s*\\(.+\\))", Pattern.CASE_INSENSITIVE);
    private static Pattern pattern = Pattern.compile("\\(\\((?<value>[+,-]?\\d+(?:,\\s*\\d+)*)\\),(?<children>(?<left>\\s*\\(.+\\)),(?<right>\\s*\\(.+\\))|\\)*)", Pattern.CASE_INSENSITIVE);

    public TreeNode() {
        this.encoded = OneHotEncoder.encodeNullOperator();
        this.value = OneHotEncoder.encodeNullOperator();
    }

    public TreeNode(long[] encoded, TreeNode left, TreeNode right) {
        this.encoded = encoded;
        this.value = encoded;
        this.left = left;
        this.right = right;
    }

    public TreeNode(Operator operator, long[] encoded, TreeNode left, TreeNode right) {
        this.operator = operator;
        this.encoded = encoded;
        this.value = encoded;
        this.left = left;
        this.right = right;
    }

    @Override
    public TreeNode create() {
        return new TreeNode();
    }

    @Override
    public TreeNode getLeft() {
        return TreeNode.class.cast(left);
    }

    @Override
    public TreeNode getRight() {
        return TreeNode.class.cast(right);
    }

    @Override
    public String display() {
        return Long.toString(this.encoded[0]);
    }

    public String toStringEncoding() {
        String encodedString = Arrays.toString(encoded).replace("[", "(").replace("]", ")").replaceAll("\\s+", "");

        if (this.getLeft() == null && this.getRight() == null) {
            return '(' + encodedString + ",)";
        }

        /*
        if (left.isNullOperator() && right.operator == null) {
            return '(' + encodedString + ",)";
        }*/

        String leftString = "";
        String rightString = "";

        if (this.getLeft() != null) {
            TreeNode castLeft = this.getLeft();

            if (castLeft.isNullOperator()) {
                leftString = Arrays.toString(OneHotEncoder.encodeNullOperator()).replace("[", "((").replace("]", "),)").replaceAll("\\s+", "");
            } else {
                leftString = castLeft.toStringEncoding();
            }
        }

        if (this.getRight() != null) {
            TreeNode castRight = this.getRight();

            if (castRight.isNullOperator()) {
                rightString = Arrays.toString(OneHotEncoder.encodeNullOperator()).replace("[", "((").replace("]", "),)").replaceAll("\\s+", "");
            } else {
                rightString = castRight.toStringEncoding();
            }
        }

        /*
        if (left == null) {
            leftString = Arrays.toString(OneHotEncoder.encodeNullOperator()).replace("[", "((").replace("]", "),)").replaceAll("\\s+", "");
        } else {
            leftString = left.toString();
        }

        if (right == null) {
            rightString = Arrays.toString(OneHotEncoder.encodeNullOperator()).replace("[", "((").replace("]", "),)").replaceAll("\\s+", "");
        } else {
            rightString = right.toString();
        }*/

        return "(" + encodedString + "," + leftString + "," + rightString + ")";
    }

    public static TreeNode fromString(String encoded) {
        TreeNode result = new TreeNode();
        Matcher matcher = pattern.matcher(encoded);
        String value = "";

        if (!matcher.find()) {
            return null;
        }

        value = matcher.group("value");
        String left = matcher.group("left");
        String right = matcher.group("right");
        Long[] encodedLongs = Stream.of(value.split(","))
            .map(val -> Long.valueOf(val.replaceAll("\\s+","")))
            .collect(Collectors.toList()).toArray(Long[]::new);

        // ignore if no platform choices given
        if (Stream.of(encodedLongs).reduce(0l, Long::sum) == 0) {
            return null;
        }

        result.encoded = ArrayUtils.toPrimitive(encodedLongs);

        if (left != null) {
            result.left = TreeNode.fromString(left);
        }

        if (right != null) {
            result.right = TreeNode.fromString(right);
        }

        return result;
    }

    public TreeNode withIdsFrom(TreeNode node) {
        this.encoded[0] = node.encoded[0];


        if (this.getLeft() != null && node.getLeft() != null) {
            this.left = this.getLeft().withIdsFrom(node.getLeft());
        }

        if (this.getRight() != null && node.getRight() != null) {
            this.right = this.getRight().withIdsFrom(node.getRight());
        }

        return this;
    }

    public TreeNode withPlatformChoicesFrom(TreeNode node) {
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
        HashMap<String, Integer> platformMappings = OneHotMappings.getInstance().getPlatformsMapping();
        HashMap<String, Integer> operatorMappings = OneHotMappings.getInstance().getOperatorMapping();
        int operatorsCount = operatorMappings.size();
        int platformsCount = platformMappings.size();

        if (this.encoded.length > 0) {
            // Check if this already encodes a platform specific operator
            long[] platformChoices = Arrays.copyOfRange(this.encoded, PADDING_SIZE + operatorsCount, PADDING_SIZE + operatorsCount + platformsCount);
            if (ArrayUtils.indexOf(platformChoices, 1) != -1) {
                System.out.println("Encoding while choices: " + Arrays.toString(platformChoices));
                return this;
            }
        }

        int platformPosition = -1;
        platformPosition = ArrayUtils.indexOf(node.encoded, 1);
        System.out.println("Encoding while choices: " + Arrays.toString(node.encoded));
        String platform = "";

        assert platformPosition >= 0;

        for (Map.Entry<String, Integer> pair : platformMappings.entrySet()) {
            if (pair.getValue() == platformPosition) {
                platform = pair.getKey();
            }
        }

        assert platform != "";

        this.encoded[PADDING_SIZE + operatorsCount + platformPosition] = 1;

        /*
        if (this.getLeft() != null) {
            assert node.getLeft() != null;
            this.getLeft() = left.withPlatformChoicesFrom(node.getLeft());
        }

        if (this.getRight() != null) {
            assert node.getRight() != null;
            this.getRight() = right.withPlatformChoicesFrom(node.getRight());
        }*/

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
        long[] values = Arrays.stream(this.encoded).map(value -> value == maxValue ? 1 : 0).toArray();

        /*
        for (int i = 0; i < values.length; i++) {
            if (values[i] == 1 && disallowed.contains(i)) {
                this.encoded[i] = 0;
                this.softmax();

                return;
            }
        }*/

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

    public TreeNode getNode(int index) {
        List<TreeNode> nodes = new ArrayList<>();

        //Add null operator
        nodes.add(new TreeNode());

        this.traverse((node) -> {
            //if (!((TreeNode) node).isNullOperator()) {
                nodes.add((TreeNode) node);
            //}
        });

        return nodes.get(index);
    }

    public int getNumberOfNodes() {
        List<TreeNode> nodes = new ArrayList<>();

        //Add null operator
        nodes.add(new TreeNode());

        this.traverse((node) -> {
            //if (!((TreeNode) node).isNullOperator()) {
                nodes.add((TreeNode) node);
            //}
        });

        return nodes.size();
    }
}
