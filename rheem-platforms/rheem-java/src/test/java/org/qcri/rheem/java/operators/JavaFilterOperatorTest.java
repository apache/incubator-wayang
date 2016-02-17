package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.channels.TestChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;
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
                        new PredicateDescriptor<>(i -> i > 0, DataUnitType.createBasic(Integer.class))
                );

        ChannelExecutor[] inputs = new ChannelExecutor[]{new TestChannelExecutor(inputStream)};
        ChannelExecutor[] outputs = new ChannelExecutor[]{new TestChannelExecutor()};
        filterOperator.evaluate(inputs, outputs, new FunctionCompiler());

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideStream().collect(Collectors.toList());
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(Arrays.asList(1, 1, 2, 6), result);

    }

}
