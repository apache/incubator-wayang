package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.JavaChannelInstance;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Test suite for {@link JavaLocalCallbackSink}.
 */
public class JavaLocalCallbackSinkTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        List<Integer> inputValues = Arrays.asList(1, 2, 3);

        // Build the sink.
        List<Integer> collector = new LinkedList<>();
        JavaLocalCallbackSink<Integer> sink = new JavaLocalCallbackSink<>(collector::add, DataSetType.createDefault(Integer.class));

        // Execute.
        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createCollectionChannelInstance(inputValues)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{};
        evaluate(sink, inputs, outputs);

        // Verify the outcome.
        Assert.assertEquals(collector, inputValues);
    }
}
