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

package org.apache.wayang.api.python.function;

import com.google.protobuf.ByteString;
import org.apache.wayang.api.python.executor.PythonWorkerManager;
import org.apache.wayang.core.function.FunctionDescriptor;

public class WrappedPythonFunction<Input, Output> implements FunctionDescriptor.SerializableFunction<Iterable<Input>, Iterable<Output>> {

    private ByteString serializedUDF;

    public WrappedPythonFunction(ByteString serializedUDF){
        this.serializedUDF = serializedUDF;
    }

    @Override
    public Iterable<Output> apply(Iterable<Input> input) {
        final PythonWorkerManager<Input, Output> manager = new PythonWorkerManager<>(serializedUDF, input);
        final Iterable<Output> output = manager.execute();
        return output;
    }

}
