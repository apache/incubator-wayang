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

import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.OperatorBase;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.types.DataSetType;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Dummy {@link ExecutionOperator} for test purposes.
 */
public class DummyExecutionOperator extends OperatorBase implements ExecutionOperator {

    public List<List<ChannelDescriptor>> supportedInputChannelDescriptors = new ArrayList<>();

    public List<List<ChannelDescriptor>> supportedOutputChannelDescriptors = new ArrayList<>();

    private int someProperty;

    public int getSomeProperty() {
        return someProperty;
    }

    public void setSomeProperty(int someProperty) {
        this.someProperty = someProperty;
    }

    public DummyExecutionOperator(int numInputs, int numOutputs, boolean isSupportingBroadcastInputs) {
        super(numInputs, numOutputs, isSupportingBroadcastInputs);
        for (int inputIndex = 0; inputIndex < numInputs; inputIndex++) {
            this.inputSlots[inputIndex] = new InputSlot<Object>(String.format("input%d", inputIndex), this, DataSetType.createDefault(Integer.class));
            supportedInputChannelDescriptors.add(new LinkedList<>());
        }
        for (int outputIndex = 0; outputIndex < numOutputs; outputIndex++) {
            this.outputSlots[outputIndex] = new OutputSlot<Object>(String.format("output%d", outputIndex), this, DataSetType.createDefault(Integer.class));
            supportedOutputChannelDescriptors.add(new LinkedList<>());
        }
    }

    @Override
    public Platform getPlatform() {
        return DummyPlatform.getInstance();
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return this.supportedInputChannelDescriptors.get(index);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return this.supportedOutputChannelDescriptors.get(index);
    }


}
