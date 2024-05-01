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
    public Tuple<ArrayList<long[][]>,ArrayList<long[][]>>prepareTrees(ArrayList<TreeNode> trees){
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
        if (!root.isLeaf()) {
            long ID  = root.encoded[0];
            long lID = root.left.encoded[0];
            long rID = root.right.encoded[0];

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
        if (!root.isLeaf()) return rightMost(root.right);
        return root.encoded[0];
    }

    /**
     * @param flatTrees
     * @return
     */
    private ArrayList<long[][]> padAndCombine(List<long[][]> flatTrees) {
        int secondDim = flatTrees.get(0)[0].length;                                   //find the size of a flat trees node structure
        int maxFirstDim = flatTrees.stream()
                .map(a -> a.length)
                .max(Integer::compare).get(); //we are trying to find the largest flat tree

        ArrayList<long[][]> vecs = new ArrayList<>();

        for (long[][] tree : flatTrees) {
            long[][] padding = new long[maxFirstDim][secondDim];

            for (int i = 0; i < tree.length; i++) {
                System.arraycopy(tree[i], 0, padding[i], 0, tree[i].length); //should never throw exception bc of int[][] padding = new int[maxFirstDim][secondDim];
            }

            vecs.add(padding);
        }

        return vecs;
    }

    public static void main(String[] args) {
        //matrix transpose test
        ArrayList<long[][]> arr = new ArrayList<>();

        long[][] matrix1 = new long[][]{
                {1,2,3,4},
                {5,6,7,8},
                {9,10,11,12},
                {13,14,15,16}
        };

        arr.add(matrix1);

        arr = arr.stream().map(tree -> IntStream.range(0, tree[0].length)
                .mapToObj(i -> Arrays.stream(tree).mapToLong(row -> row[i]).toArray()).toArray(long[][]::new)
        ).collect(Collectors.toCollection(ArrayList::new));

        assert(Arrays.deepEquals(arr.get(0), new long[][]{{1, 5, 9, 13},
                {2, 6, 10, 14},
                {3, 7, 11, 15},
                {4, 8, 12, 16}}));
        System.out.println("test 1: Passed");

        //System.out.println(Arrays.deepToString(arr.get(0)));

        TreeNode n1 = new TreeNode(new long[]{2, 3},null,null);
        TreeNode n2 = new TreeNode(new long[]{1, 2},null,null);
        TreeNode n3 = new TreeNode(new long[]{-3,0},n1,n2);

        TreeNode n4 = new TreeNode(new long[]{0, 1},null,null);
        TreeNode n5 = new TreeNode(new long[]{-1, 0},null,null);
        TreeNode n6 = new TreeNode(new long[]{1,2},n4,n5);

        TreeNode n7 = new TreeNode(new long[]{0,1},n6,n3);

        OrtTensorEncoder testo = new OrtTensorEncoder();

        long[][] correcto = testo.flatten(n7);

        long[][] valid = new long[][]
                {{0,0},{0,1},{1,2},{0,1},{-1,0},{-3,0},{2,3},{1,2}};
        assert Arrays.deepEquals(valid, correcto);
        System.out.println("test 2: Passed");

        ArrayList<long[][]> correcto2 = testo.padAndCombine(Collections.singletonList(valid));
        System.out.println("test 3: Passed");

        //new int[][]{{0, 0, 1, 0, -1, -3, 2, 1}, {0, 1, 2, 1, 0, 0, 3, 2}}
        assert true;
        System.out.println("test 4: Passed");

        System.out.println(Arrays.deepToString(testo.treeConvIndexes(n7)));

        System.out.println("test 5: Passed");
        assert(testo.preorderIndexes(n7,1).toString().equals("(1,(2,3,4),(5,6,7))"));

        System.out.println("test 6: Passed");

        ArrayList<TreeNode> testArr = new ArrayList<>();
        testArr.add(n7);
        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> t = testo.prepareTrees(testArr);
        t.field0.forEach(tree -> System.out.println(Arrays.deepToString(tree)));
        t.field1.forEach(tree -> System.out.println(Arrays.deepToString(tree)));
    }


    /**
     * @param root
     * @return
     */
    private long[][] flatten(TreeNode root){
        ArrayList<long[]> acc = new ArrayList<>();
        flattenStep(root,acc);


        acc.add(0, new long[acc.get(0).length]); //not sure that the size is correct.

        return acc.toArray(long[][]::new); //fix this. idk if it distributes the rows correctly.
    }

    private void flattenStep(TreeNode v, ArrayList<long[]> acc){
        if (v.isLeaf()) {
            acc.add(v.encoded);
            return;
        }

        acc.add(v.encoded);
        flattenStep(v.left, acc);
        flattenStep(v.right, acc);
    }
}
