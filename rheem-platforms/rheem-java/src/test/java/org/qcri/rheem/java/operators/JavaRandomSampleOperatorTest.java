package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.channels.TestChannelExecutor;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link JavaRandomSampleOperator}.
 */
public class JavaRandomSampleOperatorTest {

    @Test
    public void testExecution() {
        // Prepare test data.
        Stream<Integer> inputStream = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).stream();
        final int sampleSize = 3;

        // Build the distinct operator.
        JavaRandomSampleOperator<Integer> sampleOperator =
                new JavaRandomSampleOperator<>(
                        sampleSize,
                        10,
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        ChannelExecutor[] inputs = new ChannelExecutor[]{new TestChannelExecutor(inputStream)};
        ChannelExecutor[] outputs = new ChannelExecutor[]{new TestChannelExecutor()};

        // Execute.
        sampleOperator.evaluate(inputs, outputs, null);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideStream().collect(Collectors.toList());
        System.out.println(result);
        Assert.assertEquals(sampleSize, result.size());

    }

}
