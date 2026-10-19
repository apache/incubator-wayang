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

package org.apache.wayang.ml.validation;

import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.ml.encoding.TreeNode;

import com.google.common.primitives.Longs;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;

/**
 * ValidationRule to forbid going to Postgres when input has not been on
 * Postgres before
 */
public class PostgresSourceValidationRule implements ValidationRule {
    private static int indexOfMax(final Float[] array) {
        if (array == null || array.length == 0) {
            throw new IllegalArgumentException("Array must not be null or empty");
        }
        int maxIndex = 0;
        Float maxValue = array[0];

        for (int i = 1; i < array.length; i++) {
            if (array[i] > maxValue) {
                maxValue = array[i];
                maxIndex = i;
            }
        }
        return maxIndex;
    }

    /*
     * Index of platform choice for Postgres
     */
    private final int postgresIndex = 3;

    public PostgresSourceValidationRule() {
    }

    public void validate(final Float[][] choices, final long[][][] indexes, final TreeNode tree) {
        // Start at 1, 0th platform choice is for null operators
        for (int i = 1; i < choices.length; i++) {
            final Float max = Arrays.stream(choices[i]).max(Comparator.naturalOrder()).orElse(-Float.MAX_VALUE);

            // Check if Postgres is to be chosen
            if (indexOfMax(choices[i]) == postgresIndex) {
                // Check if Postgres has been chosen in one of the preceeding inputs
                if (!isPostgresAllowed(i, indexes, choices, tree)) {
                    for (int j = 0; j < choices[i].length; j++) {
                        if (max.equals(choices[i][j])) {
                            /*
                             * Set this choice to zero, identifying the platform choices later will take
                             * care of the rest
                             */
                            choices[i][j] = -Float.MAX_VALUE;
                            break;
                        }
                    }
                }
            }
        }
    }

    /*
     * Helper to retrieve the input indexes from a given index
     */
    private Tuple<Optional<Long>, Optional<Long>> getInputIndexes(final long index, final long[][][] indexes, final TreeNode tree) {
        final long[] flatIndexTree = Arrays.stream(indexes[0]).reduce(Longs::concat).orElseThrow();
        for (int i = 0; i < flatIndexTree.length; i += 3) {
            final long rootId = flatIndexTree[i];
            final long leftId = flatIndexTree[i + 1];
            final long rightId = flatIndexTree[i + 2];

            if (rootId == index) {
                final Optional<Long> left = (leftId == 0 || tree.getNode((int) leftId).isNullOperator()) ? Optional.empty()
                        : Optional.of(leftId);
                final Optional<Long> right = (rightId == 0 || tree.getNode((int) rightId).isNullOperator()) ? Optional.empty()
                        : Optional.of(rightId);

                // Optional<Long> left = leftId == 0 ? Optional.empty() : Optional.of(leftId);
                // Optional<Long> right = rightId == 0 ? Optional.empty() :
                // Optional.of(rightId);

                return new Tuple<>(left, right);
            }
        }

        return new Tuple<>(Optional.empty(), Optional.empty());
    }

    private boolean isPostgresAllowed(final int index, final long[][][] indexes, final Float[][] choices, final TreeNode tree) {
        // Check if current operator choice is on PostgreSQL
        if (indexOfMax(choices[index]) == postgresIndex) {
            // Check for all children recursively
            final Tuple<Optional<Long>, Optional<Long>> inputIndexes = getInputIndexes((long) index, indexes, tree);

            // Recurse left
            if (inputIndexes.getField0().isPresent()) {
                final int leftIndex = inputIndexes.getField0().get().intValue();
                if (!isPostgresAllowed(leftIndex, indexes, choices, tree)) {
                    return false;
                }
            }

            // Recurse right
            if (inputIndexes.getField1().isPresent()) {
                final int rightIndex = inputIndexes.getField1().get().intValue();

                if (!isPostgresAllowed(rightIndex, indexes, choices, tree)) {
                    return false;
                }
            }

            return true;
        }

        return false;
    }
}
