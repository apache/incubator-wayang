package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.JavaChannelInstance;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link JavaDistinctOperator}.
 */
public class JavaDistinctOperatorTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        Stream<Integer> inputStream = Arrays.asList(0, 1, 1, 2, 2, 6, 6).stream();

        // Build the distinct operator.
        JavaDistinctOperator<Integer> distinctOperator =
                new JavaDistinctOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Execute.
        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(inputStream)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(distinctOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideStream().collect(Collectors.toList());
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(Arrays.asList(0, 1, 2, 6), result);

    }

}
