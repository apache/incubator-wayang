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

import org.apache.wayang.core.util.Tuple;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class OrtTensorEncoder {
    /**
     * This method prepares the trees for creation of the OnnxTensor
     * @param trees
     * @return returns a tuple of (flatTrees, indexes)
     */
    public Tuple<ArrayList<long[][]>,ArrayList<long[][]>> prepareTrees(ArrayList<TreeNode> trees){
        ArrayList<long[][]> flatTrees = trees.stream()
                .map(this::flatten)
                .collect(Collectors.toCollection(ArrayList::new));

        flatTrees = padAndCombine(flatTrees);

        flatTrees = transpose(flatTrees);

        ArrayList<long[][]> indexes = trees.stream()
                .map(this::treeConvIndexes)
                .collect(Collectors.toCollection(ArrayList::new)); //weird structure
        indexes = padAndCombine(indexes);

        return new Tuple<>(flatTrees, indexes);
    }

    private static ArrayList<long[][]> transpose(ArrayList<long[][]> flatTrees) {
        return flatTrees.stream().map(tree -> IntStream.range(0, tree[0].length) //transpose matrix
                        .mapToObj(i -> Arrays.stream(tree)
                                .mapToLong(row -> row[i])
                                .toArray()).toArray(long[][]::new)
        ).collect(Collectors.toCollection(ArrayList::new));
    }

    /**
     * Create indexes that, when used as indexes into the output of `flatten`,
     * create an array such that a stride-3 1D convolution is the same as a
     * tree convolution.
     * @param root
     * @return
     */
    private long[][] treeConvIndexes(TreeNode root){
        TreeNode indexTree = preorderIndexes(root, 1);

        ArrayList<long[]> acc = new ArrayList<>(); //in place of a generator
        treeConvIndexesStep(indexTree,acc); //mutates acc

        long[] flatAcc = acc.stream()
                .flatMapToLong(Arrays::stream)
                .toArray();

        return Arrays.stream(flatAcc)
                .mapToObj(v -> new long[]{v})
                .toArray(long[][]::new);
    }


    private void treeConvIndexesStep(TreeNode root, ArrayList<long[]> acc){
        if (root == null) {
            return;
        }

        if (!root.isLeaf()) {
            long ID  = root.encoded[0];
            long lID = root.left != null ? root.left.encoded[0] : 0;
            long rID = root.right != null ? root.right.encoded[0]: 0;

            acc.add(new long[]{ID,lID,rID});
            treeConvIndexesStep(root.left,acc);
            treeConvIndexesStep(root.right,acc);
        } else {
            acc.add(new long[]{root.encoded[0],0,0});
        }
    }


    /**
     * An array containing the nodes ordered by their index.
     * @return
     */
    private ArrayList<TreeNode> orderedNodes = new ArrayList<>();

    /**
     * transforms a tree into a tree of preorder indexes
     * @return
     * @param idx needs to default to one.
     */
    private TreeNode preorderIndexes(TreeNode root, long idx){ //this method is very scary
        //System.out.println("Node: " + root + " id: " + idx);
        if (root == null) {
            return null;
        }

        orderedNodes.add(root);

        if (root.isLeaf()) {
            return new TreeNode(new long[]{idx},null,null);
        }

        TreeNode leftSubTree = preorderIndexes(root.left, idx+1);

        long maxIndexInLeftSubTree = rightMost(leftSubTree);

        TreeNode rightSubTree = preorderIndexes(root.right, maxIndexInLeftSubTree + 1);

        return new TreeNode(new long[]{idx}, leftSubTree, rightSubTree);
    }

    private long rightMost(TreeNode root){
        if (root == null) return 0;
        if (!root.isLeaf()) return rightMost(root.right);
        return root.encoded[0];
    }

    /**
     * @param flatTrees
     * @return
     */
    private ArrayList<long[][]> padAndCombine(List<long[][]> flatTrees) {
        ArrayList<long[][]> vecs = new ArrayList<>();

        if (flatTrees.get(0).length == 0) {
            return vecs;
        }

        int secondDim = flatTrees.get(0)[0].length;                                   //find the size of a flat trees node structure
        int maxFirstDim = flatTrees.stream()
                .map(a -> a.length)
                .max(Integer::compare).get(); //we are trying to find the largest flat tree


        for (long[][] tree : flatTrees) {
            long[][] padding = new long[maxFirstDim][secondDim];

            for (int i = 0; i < tree.length; i++) {
                System.arraycopy(tree[i], 0, padding[i], 0, tree[i].length); //should never throw exception bc of int[][] padding = new int[maxFirstDim][secondDim];
            }

            vecs.add(padding);
        }

        return vecs;
    }

    public static Tuple<ArrayList<long[][]>, ArrayList<long[][]>> encode(TreeNode node) {
        //matrix transpose test

        OrtTensorEncoder testo = new OrtTensorEncoder();

        assert node != null : "Node is null and can't be encoded";


        testo.treeConvIndexes(node);
        testo.preorderIndexes(node,1);

        ArrayList<TreeNode> testArr = new ArrayList<>();
        testArr.add(node);
        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> t = testo.prepareTrees(testArr);

        return t;
    }


    /**
     * @param root
     * @return
     */
    private long[][] flatten(TreeNode root){
        if (root == null) {
            return new long[0][0];
        }

        ArrayList<long[]> acc = new ArrayList<>();
        flattenStep(root,acc);

        acc.add(0, new long[acc.get(0).length]); //not sure that the size is correct.

        return acc.toArray(long[][]::new); //fix this. idk if it distributes the rows correctly.
    }

    private void flattenStep(TreeNode v, ArrayList<long[]> acc){
        if (v == null) {
            return;
        }

        if (v.isLeaf()) {
            acc.add(v.encoded);
            return;
        }

        acc.add(v.encoded);
        flattenStep(v.left, acc);
        flattenStep(v.right, acc);
    }
}
