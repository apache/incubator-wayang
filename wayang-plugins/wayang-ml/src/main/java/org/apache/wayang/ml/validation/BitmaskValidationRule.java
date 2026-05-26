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

import java.util.Set;

import org.apache.wayang.ml.encoding.TreeNode;

/**
 * ValidationRule to forbid certain platforms when input has not been on
 * Postgres before
 */
public class BitmaskValidationRule implements ValidationRule {
    /*
     * Index of disallowed platform choices
     */
    private final Set<Integer> disallowed = Set.of(0, 1);

    public BitmaskValidationRule() {
    }

    public void validate(final Float[][] choices, final long[][][] indexes, final TreeNode tree) {
        // Start at 1, 0th platform choice is for null operators
        for (int i = 1; i < choices.length; i++) {
            for (final Integer disallowedId : disallowed) {
                choices[i][disallowedId] = -Float.MAX_VALUE;
            }
        }
    }
}
