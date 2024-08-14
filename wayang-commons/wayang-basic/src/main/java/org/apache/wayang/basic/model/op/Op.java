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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Op implements Serializable {
    private static final AtomicInteger CNT = new AtomicInteger(0);

    protected final String name;
    protected final List<Op> fromList;

    // output type
    protected final DType dType;

    public Op(DType dType) {
        this.name = this.getClass().getSimpleName() + CNT.getAndIncrement();
        this.fromList = new ArrayList<>();
        this.dType = dType;
    }

    public Op(String name, DType dType) {
        this.name = name;
        this.fromList = new ArrayList<>();
        this.dType = dType;
    }

    public String getName() {
        return name;
    }

    public DType getDType() {
        return dType;
    }

    public List<Op> getFromList() {
        return fromList;
    }

    public Op with(Op... ops) {
        assert fromList.isEmpty();
        assert ops.length == this.inputsRequired();
        for (Op op : ops) {
            assert !this.name.equals(op.name);
        }

        this.fromList.addAll(Arrays.asList(ops));
        return this;
    }

    public abstract int inputsRequired();

    public enum DType {
        ANY,
        INT32,
        INT64,
        FLOAT32,
        FLOAT64,
        BYTE,
        INT16,
        BOOL
    }
}
