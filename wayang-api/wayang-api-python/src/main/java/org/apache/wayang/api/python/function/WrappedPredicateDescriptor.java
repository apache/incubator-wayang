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

import org.apache.wayang.api.python.executor.PythonWorkerManager;
import org.apache.wayang.core.function.FlatMapDescriptor;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.types.BasicDataUnitType;

import com.google.protobuf.ByteString;
import java.util.ArrayList;

public class WrappedPredicateDescriptor<Input> extends PredicateDescriptor<Input> {

    public WrappedPredicateDescriptor(
        final ByteString serializedUDF,
        BasicDataUnitType<Input> inputTypeClass,
        ProbabilisticDoubleInterval selectivity,
        LoadProfileEstimator udfLoad
    ) {
        super(
            (item) -> {
                final ArrayList<Input> input = new ArrayList<>();
                input.add(item);
                final PythonWorkerManager<Input, Object> manager = new PythonWorkerManager<>(serializedUDF, input);
                final Iterable<Object> output = manager.execute();
                if (output.iterator().hasNext()) {
                    Object next = output.iterator().next();
                    System.out.println((Integer) next == 1);
                    return ((Integer) next) == 1;
                }

                return false;
            },
            inputTypeClass,
            selectivity,
            udfLoad
        );
    }
}
