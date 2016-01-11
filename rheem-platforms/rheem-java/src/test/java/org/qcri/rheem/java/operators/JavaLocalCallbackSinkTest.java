package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSet;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
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
        JavaLocalCallbackSink<Integer> sink = new JavaLocalCallbackSink<>(collector::add, DataSet.flatAndBasic(Integer.class));

        // Execute the sink.
        final Stream<Integer> inputStream = inputValues.stream();
        final Stream[] outputStreams = sink.evaluate(new Stream[]{inputStream}, new FunctionCompiler());

        // Verify the outcome.
        Assert.assertEquals(0, outputStreams.length);
        Assert.assertEquals(collector, inputValues);
    }
}
