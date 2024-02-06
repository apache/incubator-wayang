/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.ml4all.abstraction.plan.wrappers;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.function.FunctionDescriptor;

import java.util.ArrayList;

public class ReduceWrapper<T> implements FunctionDescriptor.SerializableBinaryOperator<T> {

    @Override
    public Object apply(Object o, Object o2) {
        ArrayList<Tuple2> a = (ArrayList<Tuple2>) o;
        ArrayList<Tuple2> b = (ArrayList<Tuple2>) o2;

        if (a == null)
            return b;
        else if (b == null)
            return a;
        else
            a.addAll(b);
        return a;
    }
}
