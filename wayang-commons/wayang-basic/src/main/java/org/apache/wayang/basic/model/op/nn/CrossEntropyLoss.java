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

package org.apache.wayang.basic.model.op.nn;

import org.apache.wayang.basic.model.op.Op;

public class CrossEntropyLoss extends Op {
    private final int labels;

    public CrossEntropyLoss(int labels) {
        super(DType.FLOAT32);
        this.labels = labels;
    }

    public CrossEntropyLoss(int labels, String name) {
        super(name, DType.FLOAT32);
        this.labels = labels;
    }

    public int getLabels() {
        return labels;
    }

    @Override
    public DType getDType() {
        if (!fromList.isEmpty() && fromList.get(0).getDType() == DType.FLOAT64) {
            return DType.FLOAT64;
        }
        return DType.FLOAT32;
    }

    @Override
    public int inputsRequired() {
        return 2;
    }
}
