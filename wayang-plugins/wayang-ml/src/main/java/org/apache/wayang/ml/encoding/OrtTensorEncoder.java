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
        ArrayList<long[][]> flatTrees = new ArrayList<>();

        for (TreeNode tree : trees) {
            flatTrees.add(this.flatten(tree));
        }

        /*
            trees.stream()
                .map(this::flatten)
                .collect(Collectors.toCollection(ArrayList::new));*/

        flatTrees = padAndCombine(flatTrees);

        flatTrees = transpose(flatTrees);

        ArrayList<long[][]> indexes = trees.stream()
                .map(this::treeConvIndexes)
                .collect(Collectors.toCollection(ArrayList::new)); //weird structure
        indexes = padAndCombine(indexes);

        return new Tuple<>(flatTrees, indexes);
    }

    public static ArrayList<long[][]> transpose(ArrayList<long[][]> flatTrees) {
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
    public long[][] treeConvIndexes(TreeNode root){
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


    public void treeConvIndexesStep(TreeNode root, ArrayList<long[]> acc){
        if (root == null) {
            return;
        }

        if (root.isLeaf()) {
            acc.add(new long[]{root.encoded[0], 0, 0});
            //acc.add(new long[]{root.encoded[0]});

            return;
        }

        long ID  = root.encoded[0];
        long lID = root.getLeft() != null ? root.getLeft().encoded[0] : 0;
        long rID = root.getRight() != null ? root.getRight().encoded[0]: 0;

        acc.add(new long[]{ID, lID, rID});
        treeConvIndexesStep(root.getLeft(), acc);
        treeConvIndexesStep(root.getRight(), acc);
    }


    /**
     * An array containing the nodes ordered by their index.
     * @return
     */
    private static ArrayList<TreeNode> orderedNodes = new ArrayList<>();

    /**
     * transforms a tree into a tree of preorder indexes
     * @return
     * @param idx needs to default to one.
     */
    public TreeNode preorderIndexes(TreeNode root, long idx){ //this method is very scary - Mads, early 2024 | - True, Juri, late 2024
        if (root == null) {
            return null;
        }

        //orderedNodes.add(root);

        if (root.isNullOperator()) {
            //return new TreeNode(new long[]{0}, null, null);
            return new TreeNode(
                new long[]{idx}, null, null
            );
        }

        if (root.isLeaf()) {
            return new TreeNode(
                new long[]{idx},
                new TreeNode(new long[]{0}, null, null),
                new TreeNode(new long[]{0}, null, null)
            );
        }

        TreeNode rightSubTree = null;
        TreeNode leftSubTree = null;

        if (root.getLeft() != null) {
            leftSubTree = preorderIndexes(root.getLeft(), idx+1);
        }

        // Not that shrimple
        if (root.getRight() != null) {
            long maxIndexInLeftSubTree = rightMost(leftSubTree);
            rightSubTree = preorderIndexes(root.getRight(), maxIndexInLeftSubTree + 1);
        }

        return new TreeNode(new long[]{idx}, leftSubTree, rightSubTree);
    }

    public long rightMost(TreeNode root){
        // this null
        if (root == null) return 0;

        // left null, right null
        if (root.isLeaf()) {
            return root.encoded[0];
        }

        // left non null, right null
        if (root.getRight() == null && root.getLeft() != null) {
            return rightMost(root.getLeft());
        }

        if (root.getRight().encoded[0] == 0 && root.getLeft().encoded[0] == 0) {
            return root.encoded[0];
        }

        // Check for null operator
        if (root.getRight().encoded[0] == 0) {
            return rightMost(root.getLeft());
        }

        // left non null, right non null, this non null
        return rightMost(root.getRight());
    }

    /**
     * @param flatTrees
     * @return
     */
    public ArrayList<long[][]> padAndCombine(List<long[][]> flatTrees) {
        assert flatTrees.size() >= 1;
        //assert flatTrees.get(0).length == 2;

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


        //testo.treeConvIndexes(node);
        //testo.preorderIndexes(node,1);

        ArrayList<TreeNode> testArr = new ArrayList<>();
        testArr.add(node);
        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> t = testo.prepareTrees(testArr);

        return t;
    }


    /**
     * @param root
     * @return
     */
    public long[][] flatten(TreeNode root){
        if (root == null) {
            return new long[0][0];
        }

        ArrayList<long[]> acc = new ArrayList<>();
        flattenStep(root,acc);

        acc.add(0, new long[acc.get(0).length]); //not sure that the size is correct.

        return acc.toArray(long[][]::new); //fix this. idk if it distributes the rows correctly.
    }

    public void flattenStep(TreeNode v, ArrayList<long[]> acc){
        if (v == null) {
            return;
        }

        // Remove the 0th item - its the Id
        long[] values;

        if (v.isNullOperator()) {
            values = OneHotEncoder.encodeNullOperator();
        } else {
            values = Arrays.copyOf(v.encoded, v.encoded.length);
        }
        values[0] = 0;
        acc.add(values);

        if (v.isLeaf()) {
            return;
        }

        flattenStep(v.getLeft(), acc);
        flattenStep(v.getRight(), acc);
    }
}
