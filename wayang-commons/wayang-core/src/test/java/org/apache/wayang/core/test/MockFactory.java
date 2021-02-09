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

package org.apache.wayang.core.test;

import org.mockito.Answers;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.plan.wayangplan.CompositeOperator;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.OperatorContainer;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.types.DataSetType;

import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Utility to mock Wayang objects.
 */
public class MockFactory {

    public static Job createJob(Configuration configuration) {
        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        final DefaultOptimizationContext optimizationContext = new DefaultOptimizationContext(job);
        when(job.getOptimizationContext()).thenReturn(optimizationContext);
        return job;
    }

    public static ExecutionOperator createExecutionOperator(int numInputs, int numOutputs, Platform platform) {
        return createExecutionOperator(String.format("%d->%d, %s", numInputs, numOutputs, platform.getName()),
                numInputs, numOutputs, platform);
    }

    public static ExecutionOperator createExecutionOperator(String name, int numInputs, int numOutputs, Platform platform) {
        final ExecutionOperator mockedExecutionOperator = mock(ExecutionOperator.class, Answers.CALLS_REAL_METHODS);
        when(mockedExecutionOperator.toString()).thenReturn("ExecutionOperator[" + name + "]");
        when(mockedExecutionOperator.getPlatform()).thenReturn(platform);

        // Mock input slots.
        final InputSlot[] inputSlots = new InputSlot[numInputs];
        for (int inputIndex = 0; inputIndex < numInputs; inputIndex++) {
            inputSlots[inputIndex] = new InputSlot("input-" + inputIndex, mockedExecutionOperator, mock(DataSetType.class));
        }
        when(mockedExecutionOperator.getAllInputs()).thenReturn(inputSlots);
        when(mockedExecutionOperator.getNumInputs()).thenCallRealMethod();

        // Mock output slots.
        final OutputSlot[] outputSlots = new OutputSlot[numOutputs];
        for (int outputIndex = 0; outputIndex < numOutputs; outputIndex++) {
            outputSlots[outputIndex] = new OutputSlot("output" + outputIndex, mockedExecutionOperator, mock(DataSetType.class));
        }
        when(mockedExecutionOperator.getAllOutputs()).thenReturn(outputSlots);
        when(mockedExecutionOperator.getNumOutputs()).thenCallRealMethod();
        return mockedExecutionOperator;
    }

    public static Platform createPlatform(String name) {
        final Platform mockedPlatform = mock(Platform.class, Answers.CALLS_REAL_METHODS);
        when(mockedPlatform.getName()).thenReturn(name);
        return mockedPlatform;
    }

    public static CompositeOperator createCompositeOperator() {
        final CompositeOperator op = mock(CompositeOperator.class);
        final OperatorContainer container = mock(OperatorContainer.class);
        when(op.getContainers()).thenReturn(Collections.singleton(container));
        when(op.getContainer()).thenReturn(container);
        when(container.toOperator()).thenReturn(op);
        return op;
    }


}
