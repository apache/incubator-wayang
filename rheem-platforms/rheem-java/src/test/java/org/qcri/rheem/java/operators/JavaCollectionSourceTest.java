package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.channels.TestChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test suite for the {@link JavaCollectionSource}.
 */
public class JavaCollectionSourceTest {

    @Test
    public void testExecution() {
        Set<Integer> inputValues = new HashSet<>(Arrays.asList(1, 2, 3));
        JavaCollectionSource collectionSource = new JavaCollectionSource(
                inputValues, 
                DataSetType.createDefault(Integer.class));
        ChannelExecutor[] inputs = new ChannelExecutor[0];
        ChannelExecutor[] outputs = new ChannelExecutor[] { new TestChannelExecutor()};

        collectionSource.evaluate(inputs, outputs, new FunctionCompiler());

        final Set<Object> outputValues = outputs[0].provideStream().collect(Collectors.toSet());
        Assert.assertEquals(outputValues, inputValues);
    }


}
