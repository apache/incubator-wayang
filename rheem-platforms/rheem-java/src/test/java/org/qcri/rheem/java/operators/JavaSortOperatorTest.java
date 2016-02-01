package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link JavaSortOperator}.
 */
public class JavaSortOperatorTest {

    @Test
    public void testExecution() {
        // Prepare test data.
        Stream<Integer> inputStream = Arrays.asList(6, 0, 1, 1, 5, 2).stream();

        // Build the sort operator.
        JavaSortOperator<Integer> sortOperator =
                new JavaSortOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Execute the sort operator.
        final Stream[] outputStreams = sortOperator.evaluate(new Stream[]{inputStream}, new FunctionCompiler());

        // Verify the outcome.
        Assert.assertEquals(1, outputStreams.length);
        final List<Integer> result =
                ((Stream<Integer>) outputStreams[0]).collect(Collectors.toList());
        Assert.assertEquals(6, result.size());
        Assert.assertEquals(Arrays.asList(0, 1, 1, 2, 5, 6), result);

    }

}
