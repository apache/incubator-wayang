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
 * Test suite for {@link JavaCountOperator}.
 */
public class JavaCountOperatorTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        Stream<Integer> inputStream = Arrays.asList(1, 2, 3, 4, 5).stream();

        // Build the count operator.
        JavaCountOperator<Integer> countOperator =
                new JavaCountOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Execute.
        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(inputStream)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createCollectionChannelInstance()};
        evaluate(countOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideStream().collect(Collectors.toList());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(Long.valueOf(5), result.get(0));

    }

}
