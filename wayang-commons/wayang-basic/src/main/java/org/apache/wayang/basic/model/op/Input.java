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

    public Input() {
        this(DType.FLOAT32);
    }

    public Input(String name) {
        this(name, DType.FLOAT32);
    }

    public Input(Type type) {
        this(type.getName(), DType.FLOAT32);
    }

    public Input(DType dType) {
        super(dType);
    }

    public Input(String name, DType dType) {
        super(name, dType);
    }

    public Input(Type type, DType dType) {
        super(type.getName(), dType);
    }

    @Override
    public int inputsRequired() {
        return 0;
    }

    public enum Type {
        FEATURES("..FEATURES.."),
        LABEL("..LABEL.."),
        PREDICTED("..PREDICTED..");

        private final String name;

        Type(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
