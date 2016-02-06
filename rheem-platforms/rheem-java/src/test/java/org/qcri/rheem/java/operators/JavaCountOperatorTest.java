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
 * Test suite for {@link JavaCountOperator}.
 */
public class JavaCountOperatorTest {

    @Test
    public void testExecution() {
        // Prepare test data.
        Stream<Integer> inputStream = Arrays.asList(1, 2, 3, 4, 5).stream();

        // Build the count operator.
        JavaCountOperator<Integer> countOperator =
                new JavaCountOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Execute the count operator.
        final Stream[] outputStreams = countOperator.evaluate(new Stream[]{inputStream}, new FunctionCompiler());

        // Verify the outcome.
        Assert.assertEquals(1, outputStreams.length);
        final List<Integer> result =
                ((Stream<Integer>) outputStreams[0]).collect(Collectors.toList());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(Long.valueOf(5), result.get(0));

    }

}
