package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.channels.TestChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link JavaLocalCallbackSink}.
 */
public class JavaLocalCallbackSinkTest {

    @Test
    public void testExecution() {
        // Prepare test data.
        List<Integer> inputValues = Arrays.asList(1, 2, 3);

        // Build the sink.
        List<Integer> collector = new LinkedList<>();
        JavaLocalCallbackSink<Integer> sink = new JavaLocalCallbackSink<>(collector::add, DataSetType.createDefault(Integer.class));

        // Execute.
        ChannelExecutor[] inputs = new ChannelExecutor[]{new TestChannelExecutor(inputValues)};
        ChannelExecutor[] outputs = new ChannelExecutor[]{};
        sink.evaluate(inputs, outputs, new FunctionCompiler());

        // Verify the outcome.
        Assert.assertEquals(collector, inputValues);
    }
}
