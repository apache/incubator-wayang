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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import org.apache.wayang.core.util.Tuple;

public class OrtTensorEncoder {
    public static ArrayList<long[][]> transpose(final ArrayList<long[][]> flatTrees) {
        return flatTrees.stream().map(tree -> IntStream.range(0, tree[0].length) // transpose matrix
                .mapToObj(i -> Arrays.stream(tree).mapToLong(row -> row[i]).toArray()).toArray(long[][]::new))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    /**
     * Encodes a single tree
     * 
     * @param node root tree node
     * @return a flat struture of (trees, indexes)
     */
    public static Tuple<ArrayList<long[][]>, ArrayList<long[][]>> encode(final @Nonnull TreeNode node) {
        return OrtTensorEncoder.prepareTrees(List.of(node));
    }

    /**
     * This method prepares the trees for creation of the OnnxTensor
     * 
     * @param trees
     * @return returns a tuple of (flatTrees, indexes)
     */
    public static Tuple<ArrayList<long[][]>, ArrayList<long[][]>> prepareTrees(final List<TreeNode> trees) {
        final List<long[][]> flatTrees = trees.stream().map(OrtTensorEncoder::flatten).toList();

        final ArrayList<long[][]> paddedTrees = padAndCombine(flatTrees);

        final ArrayList<long[][]> transposedTrees = transpose(paddedTrees);

        final ArrayList<long[][]> indexes = trees.stream().map(OrtTensorEncoder::treeConvIndexes)
                .collect(Collectors.toCollection(ArrayList::new));

        final ArrayList<long[][]> paddedIndexes = padAndCombine(indexes);

        return new Tuple<>(transposedTrees, paddedIndexes);
    }

    /**
     * Create indexes that, when used as indexes into the output of `flatten`,
     * create an array such that a stride-3 1D convolution is the same as a tree
     * convolution.
     * 
     * @param root
     * @return
     */
    public static long[][] treeConvIndexes(final TreeNode root) {
        final TreeNode indexTree = preorderIndexes(root, 1);

        final ArrayList<long[]> acc = new ArrayList<>(); // in place of a generator
        treeConvIndexesStep(indexTree, acc); // mutates acc

        final long[] flatAcc = acc.stream().flatMapToLong(Arrays::stream).toArray();

        return Arrays.stream(flatAcc).mapToObj(v -> new long[] { v }).toArray(long[][]::new);
    }

    public static void treeConvIndexesStep(final TreeNode root, final ArrayList<long[]> acc) {
        if (root == null) {
            return;
        }

        if (root.isLeaf()) {
            acc.add(new long[] { root.encoded[0], 0, 0 });
            return;
        }

        final long ID = root.encoded[0];
        final long lID = root.getLeft() != null ? root.getLeft().encoded[0] : 0;
        final long rID = root.getRight() != null ? root.getRight().encoded[0] : 0;

        acc.add(new long[] { ID, lID, rID });
        treeConvIndexesStep(root.getLeft(), acc);
        treeConvIndexesStep(root.getRight(), acc);
    }

    /**
     * transforms a tree into a tree of preorder indexes
     * 
     * @return
     * @param idx needs to default to one.
     */
    public static TreeNode preorderIndexes(final TreeNode root, final long idx) {
        if (root == null) {
            return null;
        }

        if (root.isNullOperator()) {
            return new TreeNode(new long[] { idx }, null, null);
        }

        if (root.isLeaf()) {
            return new TreeNode(new long[] { idx }, new TreeNode(new long[] { 0 }, null, null),
                    new TreeNode(new long[] { 0 }, null, null));
        }

        final TreeNode leftSubTree = root.getLeft() != null ? preorderIndexes(root.getLeft(), idx + 1) : null;

        final TreeNode rightSubTree = root.getRight() != null
                ? preorderIndexes(root.getRight(), rightMost(leftSubTree) + 1)
                : null;

        return new TreeNode(new long[] { idx }, leftSubTree, rightSubTree);
    }

    public static long rightMost(final TreeNode root) {
        if (root == null)
            return 0;

        if (root.isLeaf()) {
            return root.encoded[0];
        }

        if (root.getRight() == null && root.getLeft() != null) {
            return rightMost(root.getLeft());
        }

        if (root.getRight().encoded[0] == 0 && root.getLeft().encoded[0] == 0) {
            return root.encoded[0];
        }

        if (root.getRight().encoded[0] == 0) {
            return rightMost(root.getLeft());
        }

        return rightMost(root.getRight());
    }

    /**
     * @param flatTrees
     * @return
     */
    public static ArrayList<long[][]> padAndCombine(final List<long[][]> flatTrees) {
        assert flatTrees.size() >= 1;

        final ArrayList<long[][]> vecs = new ArrayList<>();

        if (flatTrees.get(0).length == 0) {
            return vecs;
        }

        final int secondDim = flatTrees.get(0)[0].length;
        final int maxFirstDim = flatTrees.stream().mapToInt(a -> a.length).max().orElseThrow();

        for (final long[][] tree : flatTrees) {
            final long[][] padding = new long[maxFirstDim][secondDim];

            for (int i = 0; i < tree.length; i++) {
                System.arraycopy(tree[i], 0, padding[i], 0, tree[i].length);
            }

            vecs.add(padding);
        }

        return vecs;
    }

    /**
     * @param root
     * @return
     */
    public static long[][] flatten(final TreeNode root) {
        if (root == null) {
            return new long[0][0];
        }

        final ArrayList<long[]> acc = new ArrayList<>();
        flattenStep(root, acc);

        acc.add(0, new long[acc.get(0).length]);

        return acc.toArray(long[][]::new);
    }

    public static void flattenStep(final TreeNode v, final ArrayList<long[]> acc) {
        if (v == null) {
            return;
        }

        final long[] values = v.isNullOperator() ? OneHotEncoder.encodeNullOperator()
                : Arrays.copyOf(v.encoded, v.encoded.length);

        values[0] = 0;
        acc.add(values);

        if (v.isLeaf()) {
            return;
        }

        flattenStep(v.getLeft(), acc);
        flattenStep(v.getRight(), acc);
    }
}
