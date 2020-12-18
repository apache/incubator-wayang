package org.apache.incubator.wayang.core.test;

import org.apache.incubator.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.incubator.wayang.core.plan.wayangplan.InputSlot;
import org.apache.incubator.wayang.core.plan.wayangplan.OperatorBase;
import org.apache.incubator.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.incubator.wayang.core.platform.ChannelDescriptor;
import org.apache.incubator.wayang.core.platform.Platform;
import org.apache.incubator.wayang.core.types.DataSetType;

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
