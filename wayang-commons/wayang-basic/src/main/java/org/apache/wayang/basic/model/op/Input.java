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

public class Input extends Op {
    private final int[] shape;

    public Input(int[] shape) {
        this(shape, DType.FLOAT32);
    }

    public Input(int[] shape, String name) {
        this(shape, name, DType.FLOAT32);
    }

    public Input(int[] shape, Type type) {
        this(shape, type.getName(), DType.FLOAT32);
    }

    public Input(int[] shape, DType dType) {
        this(shape, (String) null, dType);
    }

    public Input(int[] shape, Type type, DType dType) {
        this(shape, type.getName(), dType);
    }

    public Input(int[] shape, String name, DType dType) {
        super(name, dType);
        this.shape = shape;
    }

    @Override
    public int inputsRequired() {
        return 0;
    }

    public int[] getShape() {
        return shape;
    }

    public enum Type {
        FEATURES("..FEATURES.."),
        LABEL("..LABEL..");

        private final String name;

        Type(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
