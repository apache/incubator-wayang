package io.rheem.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import io.rheem.rheem.core.function.TransformationDescriptor;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.java.channels.JavaChannelInstance;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link JavaSortOperator}.
 */
public class JavaSortOperatorTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        Stream<Integer> inputStream = Arrays.asList(6, 0, 1, 1, 5, 2).stream();

        // Build the sort operator.
        JavaSortOperator<Integer, Integer> sortOperator =
                new JavaSortOperator<>(new TransformationDescriptor<Integer, Integer>(
                        r->r,
                        Integer.class, Integer.class),
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Execute.
        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(inputStream)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(sortOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideStream().collect(Collectors.toList());
        Assert.assertEquals(6, result.size());
        Assert.assertEquals(Arrays.asList(0, 1, 1, 2, 5, 6), result);

    }

}
