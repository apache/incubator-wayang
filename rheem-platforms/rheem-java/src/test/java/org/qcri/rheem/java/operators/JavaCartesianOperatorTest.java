package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.channels.TestChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link JavaCartesianOperator}.
 */
public class JavaCartesianOperatorTest {

    @Test
    public void testExecution() {
        // Prepare test data.
        Stream<Integer> inputStream0 = Arrays.asList(1, 2).stream();
        Stream<String> inputStream1 = Arrays.asList("a", "b", "c").stream();

        // Build the Cartesian operator.
        JavaCartesianOperator<Integer, String> cartesianOperator =
                new JavaCartesianOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class),
                        DataSetType.createDefaultUnchecked(String.class));

        // Execute.
        ChannelExecutor[] inputs = new ChannelExecutor[]{
                new TestChannelExecutor(inputStream0),
                new TestChannelExecutor(inputStream1)
        };
        ChannelExecutor[] outputs = new ChannelExecutor[]{new TestChannelExecutor()};
        cartesianOperator.evaluate(inputs, outputs, new FunctionCompiler());

        // Verify the outcome.
        final List<Tuple2<Integer, String>> result = outputs[0].<Tuple2<Integer, String>>provideStream()
                .collect(Collectors.toList());
        Assert.assertEquals(6, result.size());
        Assert.assertEquals(result.get(0), new Tuple2(1, "a"));

    }

}
