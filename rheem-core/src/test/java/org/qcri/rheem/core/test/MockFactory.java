package org.qcri.rheem.core.test;

import org.mockito.Answers;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.types.DataSetType;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Utility to mock Rheem objects.
 */
public class MockFactory {

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




}
