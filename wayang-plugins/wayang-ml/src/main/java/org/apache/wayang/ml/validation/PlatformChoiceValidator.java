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

import java.util.Arrays;
import java.util.Comparator;

import org.apache.wayang.ml.encoding.TreeNode;
/**
 * Class used for enforcing validation rules on given platform choices
 */
public class PlatformChoiceValidator {

    public static long[][] validate(
        final float[][][] tensor,
        final long[][][] indexes,
        final TreeNode tree,
        final ValidationRule... rules
    ) {
        final Float[][] transposed = transpose(tensor);

        for (final ValidationRule rule : rules) {
            rule.validate(transposed, indexes, tree);
        }

        return getPlatformChoices(transposed);
    }

    public static Float[][] transpose(final float[][][] tensor) {
        final int cols = tensor[0][0].length;
        final int rows = tensor[0].length;
        final Float[][] transposed = new Float[cols][rows];


        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                transposed[j][i] = tensor[0][i][j];
            }
        }

        return transposed;
    }

    public static long[][] getPlatformChoices(final Float[][] transposed) {
        return Arrays.stream(transposed)
            .map(row -> {
                final Float max = Arrays.stream(row).max(Comparator.naturalOrder()).orElse(-Float.MAX_VALUE);
                final long[] result = Arrays.stream(row)
                        .mapToLong(v -> v.equals(max) ? 1L : 0L)
                        .toArray();

                return result;
            })
            .toArray(long[][]::new);
    }
}
