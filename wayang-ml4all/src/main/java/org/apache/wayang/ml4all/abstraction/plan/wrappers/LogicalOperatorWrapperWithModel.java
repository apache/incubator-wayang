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

import org.apache.wayang.core.function.ExecutionContext;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.ml4all.abstraction.plan.ML4allModel;

/**
 * Logical Operator that uses the [[org.apache.wayang.ml4all.abstraction.plan.ML4allModel]]  as input besides the data flowing from its input slot.
 * The model is broadcasted to the operator.
 */

public abstract class LogicalOperatorWrapperWithModel<R, V> implements FunctionDescriptor.ExtendedSerializableFunction<V, R> {

    protected ML4allModel ml4allModel;
    int currentIteration;
    private boolean first = true;

    @Override
    public void open(ExecutionContext executionContext) {
        currentIteration = executionContext.getCurrentIteration();
        ml4allModel = executionContext.<ML4allModel>getBroadcast("model").iterator().next();
        if (this instanceof ComputePerPartitionWrapper) {
            if (first) {
                initialise();
                first = false;
            }
            else {
                first = true;
            }
        }
    }


    public abstract void initialise();

    public void finalise() { }
}
