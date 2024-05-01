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
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class OrtTensorDecoder {
    private HashMap<Long, TreeNode> nodeToIDMap = new HashMap<>();

    public static void main(String[] args) {
        OrtTensorEncoder testo = new OrtTensorEncoder();

        TreeNode n1 = new TreeNode(new long[]{2, 3},null,null);
        TreeNode n2 = new TreeNode(new long[]{1, 2},null,null);
        TreeNode n3 = new TreeNode(new long[]{-3,0},n1,n2);

        TreeNode n4 = new TreeNode(new long[]{0, 1},null,null);
        TreeNode n5 = new TreeNode(new long[]{-1, 0},null,null);
        TreeNode n6 = new TreeNode(new long[]{1,2},n4,n5);

        TreeNode n7 = new TreeNode(new long[]{0,1},n6,n3);

        ArrayList<TreeNode> testArr = new ArrayList<>();
        testArr.add(n7);

        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> t = testo.prepareTrees(testArr);

        OrtTensorDecoder testo2 = new OrtTensorDecoder();
        System.out.println(testo2.decode(t));
    }

    //TODO: figure out output structure, from ml model
    /**
     * Decodes the output from a tree based NN model
     * @param mlOutput takes the out put from @
     */
    public TreeNode decode(Tuple<ArrayList<long[][]>,ArrayList<long[][]>> mlOutput){
        ArrayList<long[][]> valueStructure = mlOutput.field0;
        ArrayList<long[][]> indexStructure = mlOutput.field1;

        for (int i = 0; i < valueStructure.size(); i++) { //iterate for each tree, in practice should only be 1
            long[][] values      = valueStructure.get(i);
            long[][] indexedTree = indexStructure.get(i);
            long[] flatIndexTree = Arrays.stream(indexedTree).reduce(Longs::concat).orElseThrow();

            for (int j = 0; j < flatIndexTree.length; j+=3) {
                final long curID = flatIndexTree[j];
                long lID   = flatIndexTree[j+1];
                long rID   = flatIndexTree[j+2];

                long[] value = Arrays.stream(values)
                        .flatMapToLong(arr -> LongStream.of(arr[(int) curID]))
                        .toArray();

                //fetch l,r from map such that we can reference values.
                TreeNode l       = nodeToIDMap.containsKey(lID)   ? nodeToIDMap.get(lID)   : new TreeNode();
                TreeNode r       = nodeToIDMap.containsKey(rID)   ? nodeToIDMap.get(rID)   : new TreeNode();
                TreeNode curTreeNode = nodeToIDMap.containsKey(curID) ? nodeToIDMap.get(curID) : new TreeNode(value, l, r);

                //set values
                curTreeNode.encoded = value;
                curTreeNode.left     = l;
                curTreeNode.right     = r;

                //put values back into map so we can look them up in next loop
                nodeToIDMap.put(curID,curTreeNode);
                nodeToIDMap.put(lID,l);
                nodeToIDMap.put(rID,r);
            }
        }

        return this.nodeToIDMap.get(1L);
    }
}
