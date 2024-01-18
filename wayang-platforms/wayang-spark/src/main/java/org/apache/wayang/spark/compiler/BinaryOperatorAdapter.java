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

package org.apache.wayang.spark.compiler;

import org.apache.spark.api.java.function.Function2;

import java.util.function.BinaryOperator;

/**
 * Wraps a {@link java.util.function.BinaryOperator} as a {@link Function2}.
 */
public class BinaryOperatorAdapter<Type> implements Function2<Type, Type, Type> {

    private BinaryOperator<Type> binaryOperator;

    public BinaryOperatorAdapter(BinaryOperator<Type> binaryOperator) {
        this.binaryOperator = binaryOperator;
    }

    @Override
    public Type call(Type dataQuantum0, Type dataQuantum1) throws Exception {
        return this.binaryOperator.apply(dataQuantum0, dataQuantum1);
    }
}
