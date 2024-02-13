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

import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.operators.JavaExecutionOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * {@link OperatorProfiler} specifically for {@link JavaExecutionOperator}s with a single {@link InputSlot}.
 */
public class BinaryOperatorProfiler extends OperatorProfiler {

    private JavaChannelInstance inputChannelInstance0, inputChannelInstance1, outputChannelInstance;

    public BinaryOperatorProfiler(Supplier<JavaExecutionOperator> operatorGenerator,
                                  Supplier<?> dataQuantumGenerator0,
                                  Supplier<?> dataQuantumGenerator1) {
        super(operatorGenerator, dataQuantumGenerator0, dataQuantumGenerator1);
    }

    public void prepare(long... inputCardinalities) {
        assert inputCardinalities.length == 2;

        super.prepare(inputCardinalities);

        // Create operator.
        assert inputCardinalities.length == this.operator.getNumInputs();
        int inputCardinality0 = (int) inputCardinalities[0];
        int inputCardinality1 = (int) inputCardinalities[1];

        // Create input data.
        Collection<Object> dataQuanta0 = new ArrayList<>(inputCardinality0);
        final Supplier<?> supplier0 = this.dataQuantumGenerators.get(0);
        for (int i = 0; i < inputCardinality0; i++) {
            dataQuanta0.add(supplier0.get());
        }
        this.inputChannelInstance0 = createChannelInstance(dataQuanta0);

        Collection<Object> dataQuanta1 = new ArrayList<>(inputCardinality1);
        final Supplier<?> supplier1 = this.dataQuantumGenerators.get(1);
        for (int i = 0; i < inputCardinality1; i++) {
            dataQuanta1.add(supplier1.get());
        }
        this.inputChannelInstance1 = createChannelInstance(dataQuanta1);

        // Allocate output.
        this.outputChannelInstance = createChannelInstance();
    }


    public long executeOperator() {
        this.evaluate(
                new JavaChannelInstance[]{this.inputChannelInstance0, this.inputChannelInstance1},
                new JavaChannelInstance[]{this.outputChannelInstance}
        );
        return this.outputChannelInstance.provideStream().count();
    }

    @Override
    public JavaExecutionOperator getOperator() {
        return this.operator;
    }
}
