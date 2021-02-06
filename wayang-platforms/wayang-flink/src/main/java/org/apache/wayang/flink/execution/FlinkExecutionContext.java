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

package org.apache.wayang.flink.execution;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.wayang.core.function.ExecutionContext;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.flink.operators.FlinkExecutionOperator;
import org.apache.wayang.flink.platform.FlinkPlatform;

import java.io.Serializable;
import java.util.Collection;

/**
 * {@link ExecutionContext} implementation for the {@link FlinkPlatform}.
 */
public class FlinkExecutionContext implements ExecutionContext, Serializable {

    private transient FlinkExecutionOperator operator;

    private transient final ChannelInstance[] inputs;

    private transient int iterationNumber;

    private RichFunction richFunction;


    public FlinkExecutionContext(FlinkExecutionOperator operator, ChannelInstance[] inputs, int iterationNumber) {
        this.operator = operator;
        this.inputs = inputs;
        this.iterationNumber = iterationNumber;
    }


    @Override
    @SuppressWarnings("unchecked")
    public <Type> Collection<Type> getBroadcast(String name) {
        return this.richFunction.getRuntimeContext().getBroadcastVariable(name);
    }

    public void setRichFunction(RichFunction richFunction){
        this.richFunction = richFunction;
    }

    @Override
    public int getCurrentIteration() {
        return this.iterationNumber;
    }
}
