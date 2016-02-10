package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link JavaReduceByOperator}.
 */
public class JavaCoalesceOperatorTest {

    @Test
    public void testExecution() {
        // Prepare test data.
        Stream<String> inputStream1 = Arrays.stream("aabc".split(""));
        Stream<String> inputStream2 = Arrays.stream("cdef".split(""));

        // Build the operator.
        JavaCoalesceOperator<String> coalesceOperator = new JavaCoalesceOperator<>(DataSetType.createDefault(String.class));

        // Execute the operator.
        final Stream[] outputStreams = coalesceOperator.evaluate(new Stream[]{inputStream1, inputStream2}, new FunctionCompiler());

        // Verify the outcome.
        Assert.assertEquals(1, outputStreams.length);
        final List<String> result =
                ((Stream<String>) outputStreams[0]).collect(Collectors.toList());
        final String[] expectedResults = "aabccdef".split("");
        Arrays.stream(expectedResults)
                .forEach(expected -> Assert.assertTrue("Not contained: " + expected, result.contains(expected)));
        Assert.assertEquals(expectedResults.length, result.size());

    }
}
