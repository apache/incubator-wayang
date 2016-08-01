package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link JavaReservoirSampleOperator}.
 */
public class JavaReservoirSampleOperatorTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        Stream<Integer> inputStream = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).stream();
        final int sampleSize = 5;

        // Build the distinct operator.
        JavaReservoirSampleOperator<Integer> sampleOperator =
                new JavaReservoirSampleOperator<>(
                        sampleSize,
                        10,
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(inputStream)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createCollectionChannelInstance()};

        // Execute.
        sampleOperator.evaluate(inputs, outputs, (FunctionCompiler) null);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideStream().collect(Collectors.toList());
        System.out.println(result);
        Assert.assertEquals(sampleSize, result.size());

    }

}
