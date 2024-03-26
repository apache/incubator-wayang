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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.ArrayUtils;

public class TreeNode {
    public long[] encoded;
    public TreeNode left;
    public TreeNode right;
    public boolean isRoot;
    private static Pattern pattern = Pattern.compile("\\(\\((?<value>[+,-]?\\d+(?:,\\s*\\d+)*)\\),(?<left>\\s*\\(.+\\)),(?<right>\\s*\\(.+\\))", Pattern.CASE_INSENSITIVE);

    @Override
    public String toString() {
        String encodedString = Arrays.toString(encoded).replace("[", "(").replace("]", ")");

        return "(" +
          encodedString +
          ',' + (left != null ? left.toString() : Arrays.toString(OneHotEncoder.encodeNullOperator()).replace("[", "(").replace("]", ")")) + ',' +
          (right != null ? right.toString() : Arrays.toString(OneHotEncoder.encodeNullOperator()).replace("[", "(").replace("]", ")")) +
          ')';
    }

    public static TreeNode fromString(String encoded) {
        TreeNode result = new TreeNode();
        Matcher matcher = pattern.matcher(encoded);
        String value = "";

        if (!matcher.find()) {
            return null;
        }

        value = matcher.group("value");
        Long[] encodedLongs = Stream.of(value.split(","))
            .map(val -> Long.valueOf(val.replaceAll("\\s","")))
            .collect(Collectors.toList()).toArray(Long[]::new);
        result.encoded = ArrayUtils.toPrimitive(encodedLongs);
        result.left = TreeNode.fromString(matcher.group("left"));
        result.right = TreeNode.fromString(matcher.group("right"));

        return result;
    }

    public TreeNode withIdsFrom(TreeNode node) {
        this.encoded[0] = node.encoded[0];

        if (this.left != null && node.left != null) {
            this.left = left.withIdsFrom(node.left);
        }

        if (this.right != null && node.right != null) {
            this.right = right.withIdsFrom(node.right);
        }

        return this;
    }

}
