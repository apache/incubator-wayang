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
 * Test suite for {@link JavaUnionAllOperator}.
 */
public class JavaUnionAllOperatorTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        Stream<Integer> inputStream0 = Arrays.asList(6, 0, 1, 1, 5, 2).stream();
        Stream<Integer> inputStream1 = Arrays.asList(1, 1, 9).stream();

        // Build the UnionAll operator.
        JavaUnionAllOperator<Integer> unionAllOperator =
                new JavaUnionAllOperator<>(DataSetType.createDefaultUnchecked(Integer.class));

        // Execute.
        JavaChannelInstance[] inputs = new JavaChannelInstance[]{
                createStreamChannelInstance(inputStream0),
                createStreamChannelInstance(inputStream1)
        };
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(unionAllOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideStream()
                .collect(Collectors.toList());
        Assert.assertEquals(9, result.size());
        Assert.assertEquals(Arrays.asList(6, 0, 1, 1, 5, 2, 1, 1, 9), result);

    }

}
