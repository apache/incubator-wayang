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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import org.apache.wayang.core.util.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class OrtTensorDecoder {
    private HashMap<Long, TreeNode> nodeToIDMap = new HashMap<>();
    private HashMap<Long, TreeNode> visitedRoots = new HashMap<>();

    //TODO: figure out output structure, from ml model
    /**
     * Decodes the output from a tree based NN model
     * @param mlOutput takes the out put from @
     */
    public TreeNode decode(Tuple<ArrayList<long[][]>,ArrayList<long[][]>> mlOutput){
        long[][] platformChoices = mlOutput.field0.get(0);
        long[][] indexedTree = mlOutput.field1.get(0);
        long[] flatIndexTree = Arrays.stream(indexedTree).reduce(Longs::concat).orElseThrow();

        /*
        //transpose values
        // Assume values is a 2D long array: long[][] values = mlOutput.field0.get(0);
        int rows = values.length;
        int cols = values[0].length;
        long[][] transposed = new long[cols][rows];

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                transposed[j][i] = values[i][j];
            }
        }

        long[][] platformChoices = Arrays.stream(transposed)
            .map(row -> {
                long max = Arrays.stream(row).max().orElse(Long.MIN_VALUE);
                return Arrays.stream(row)
                        .map(v -> v == max ? 1L : 0L)
                        .toArray();
            })
            .toArray(long[][]::new);
        */

        //System.out.println("Flat index tree: " + Arrays.toString(flatIndexTree));
        //System.out.println("Flat index tree length: " + flatIndexTree.length);

        for (int j = 0; j < flatIndexTree.length; j+=3) {
            final long curID = flatIndexTree[j];

            //System.out.println("Looking at ID " + curID);

            // Skip over roots that have been visited before
            /*
            if (visitedRoots.containsKey(curID)) {
                continue;
            }

            visitedRoots.put(curID, null);*/

            // Skip 0s
            /*
            if (curID == 0) {
                System.out.println("Skipping 0");
                continue;
            }*/

            /*
            long[] value = Arrays.stream(values)
                    .flatMapToLong(arr -> LongStream.of(arr[(int) curID]))
                    .toArray();*/
            long[] value = platformChoices[(int) curID];

            /* //Skip 0s
            if (LongStream.of(value).reduce(0l, Long::sum) == 0) {
                System.out.println("SKIPPING 0s");
                continue;
            }*/


            //set values
            //fetch l,r from map such that we can reference values.
            TreeNode curTreeNode = nodeToIDMap.containsKey(curID) ? nodeToIDMap.get(curID) : new TreeNode(value, null, null);

            // Skip Nulloperator
            /*
            if (curTreeNode.isNullOperator()) {
                System.out.println("SKIPPING Nulloperator");
                continue;
            }*/

            curTreeNode.encoded = value;

            if (flatIndexTree.length > j+1) {
                long lID = flatIndexTree[j+1];
                TreeNode left;

                /*
                long[] lValues = Arrays.stream(values)
                        .flatMapToLong(arr -> LongStream.of(arr[(int) lID]))
                        .toArray();*/
                long[] lValues = platformChoices[(int) lID];

                if (nodeToIDMap.containsKey(lID)) {
                    left = nodeToIDMap.get(lID);
                } else {
                    left = new TreeNode(lValues, null, null);
                }

                left.encoded = lValues;

                //if (lID != 0) {
                    nodeToIDMap.put(lID, left);
                //}

                curTreeNode.left = left;

                if (flatIndexTree.length > j+2) {
                    long rID = flatIndexTree[j+2];
                    TreeNode right;

                    /*
                    long[] rValues = Arrays.stream(values)
                            .flatMapToLong(arr -> LongStream.of(arr[(int) rID]))
                            .toArray();*/

                    long[] rValues = platformChoices[(int) rID];

                    if (nodeToIDMap.containsKey(rID)) {
                        right = nodeToIDMap.get(rID);
                    } else {
                        right = new TreeNode(rValues, null, null);
                    }

                    right.encoded = rValues;

                    //if (rID != 0) {
                        nodeToIDMap.put(rID, right);
                    //}

                    curTreeNode.right = right;
                }
            }

            //put values back into map so we can look them up in next loop
            nodeToIDMap.put(curID, curTreeNode);
        }

        return this.nodeToIDMap.get(1L);
    }
}
