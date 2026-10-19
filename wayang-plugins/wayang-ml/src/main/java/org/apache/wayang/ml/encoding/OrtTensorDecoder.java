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

import org.apache.wayang.core.util.Tuple;

import com.google.common.primitives.Longs;

public class OrtTensorDecoder {

    /**
     * Decodes the output from a tree based NN model
     * 
     * @param mlOutput takes the out put from @
     */
    public static TreeNode decode(final Tuple<ArrayList<long[][]>, ArrayList<long[][]>> mlOutput) {
        final HashMap<Long, TreeNode> nodeToIDMap = new HashMap<>();
        final long[][] platformChoices = mlOutput.field0.get(0);
        final long[][] indexedTree = mlOutput.field1.get(0);
        final long[] flatIndexTree = Arrays.stream(indexedTree).reduce(Longs::concat).orElseThrow();

        for (int j = 0; j < flatIndexTree.length; j += 3) {
            final long curID = flatIndexTree[j];
            final long[] value = platformChoices[(int) curID];

            final TreeNode curTreeNode = nodeToIDMap.containsKey(curID) ? nodeToIDMap.get(curID)
                    : new TreeNode(value, null, null);

            curTreeNode.encoded = value;

            if (flatIndexTree.length > j + 1) {
                final long lID = flatIndexTree[j + 1];
                TreeNode left;

                final long[] lValues = platformChoices[(int) lID];

                if (nodeToIDMap.containsKey(lID)) {
                    left = nodeToIDMap.get(lID);
                } else {
                    left = new TreeNode(lValues, null, null);
                }

                left.encoded = lValues;

                nodeToIDMap.put(lID, left);

                curTreeNode.left = left;

                if (flatIndexTree.length > j + 2) {
                    final long rID = flatIndexTree[j + 2];
                    TreeNode right;

                    final long[] rValues = platformChoices[(int) rID];

                    if (nodeToIDMap.containsKey(rID)) {
                        right = nodeToIDMap.get(rID);
                    } else {
                        right = new TreeNode(rValues, null, null);
                    }

                    right.encoded = rValues;

                    nodeToIDMap.put(rID, right);

                    curTreeNode.right = right;
                }
            }

            // put values back into map so we can look them up in next loop
            nodeToIDMap.put(curID, curTreeNode);
        }

        return nodeToIDMap.get(1L);
    }
}
