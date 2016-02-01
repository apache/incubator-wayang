package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link JavaFilterOperator}.
 */
public class JavaFilterOperatorTest {

    @Test
    public void testExecution() {
        // Prepare test data.
        Stream<Integer> inputStream = Arrays.asList(0, 1, 1, 2, 6).stream();

        // Build the distinct operator.
        JavaFilterOperator<Integer> filterOperator =
                new JavaFilterOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class),
                        new Predicate<Integer>() {
                            @Override
                            public boolean test(Integer item) {
                                if (item > 0)
                                    return true;
                                else
                                    return false;
                            }
                        }
                );

        // Execute the distinct operator.
        final Stream[] outputStreams = filterOperator.evaluate(new Stream[]{inputStream}, new FunctionCompiler());

        // Verify the outcome.
        Assert.assertEquals(1, outputStreams.length);
        final List<Integer> result =
                ((Stream<Integer>) outputStreams[0]).collect(Collectors.toList());
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(Arrays.asList(1, 1, 2, 6), result);

    }

}
