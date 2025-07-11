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

package org.apache.wayang.basic.model.op;

public class Slice extends Op {
    private final int[][] range; // int[dim][2]

    public Slice(int[][] range) {
        this(range, null, DType.FLOAT32);
    }

    public Slice(int[][] range, DType dType) {
        this(range, null, dType);
    }

    public Slice(int[][] range, String name) {
        this(range, name, DType.FLOAT32);
    }

    public Slice(int[][] range, String name, DType dType) {
        super(name, dType);
        this.range = range;
    }

    public int[][] getRange() {
        return range;
    }

    @Override
    public int inputsRequired() {
        return 1;
    }
}
