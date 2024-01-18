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

package org.apache.wayang.profiler.java;

import org.apache.commons.lang3.Validate;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.operators.JavaExecutionOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * {@link OperatorProfiler} specifically for {@link JavaExecutionOperator}s with a single {@link InputSlot}.
 */
public class UnaryOperatorProfiler extends OperatorProfiler {

    private JavaChannelInstance inputChannelInstance, outputChannelInstance;

    public UnaryOperatorProfiler(Supplier<JavaExecutionOperator> operatorGenerator, Supplier<?> dataQuantumGenerator) {
        super(operatorGenerator, dataQuantumGenerator);
    }

    public void prepare(long... inputCardinalities) {
        Validate.isTrue(inputCardinalities.length == 1);

        super.prepare(inputCardinalities);
        int inputCardinality = (int) inputCardinalities[0];

        // Create input data.
        Collection<Object> dataQuanta = new ArrayList<>(inputCardinality);
        final Supplier<?> supplier = this.dataQuantumGenerators.get(0);
        for (int i = 0; i < inputCardinality; i++) {
            dataQuanta.add(supplier.get());
        }
        this.inputChannelInstance = createChannelInstance(dataQuanta);

        // Allocate output.
        this.outputChannelInstance = createChannelInstance();
    }


    public long executeOperator() {
        this.evaluate(
                new JavaChannelInstance[]{this.inputChannelInstance},
                new JavaChannelInstance[]{this.outputChannelInstance}
        );
        return this.outputChannelInstance.provideStream().count();
    }

    @Override
    public JavaExecutionOperator getOperator() {
        return this.operator;
    }
}
