package org.qcri.rheem.core.test;

import org.mockito.Answers;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.Platform;

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
        when(mockedExecutionOperator.getAllInputs()).thenReturn(new InputSlot[numInputs]);
        when(mockedExecutionOperator.getNumInputs()).thenCallRealMethod();
        when(mockedExecutionOperator.getAllOutputs()).thenReturn(new OutputSlot[numOutputs]);
        return mockedExecutionOperator;
    }

    public static Platform createPlatform(String name) {
        final Platform mockedPlatform = mock(Platform.class, Answers.CALLS_REAL_METHODS);
        when(mockedPlatform.getName()).thenReturn(name);
        return mockedPlatform;
    }




}
