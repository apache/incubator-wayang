package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.operators.UDFSampleSize;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.JavaChannelInstance;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link JavaRandomSampleOperator}.
 */
public class JavaRandomSampleOperatorTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        Stream<Integer> inputStream = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).stream();
        final int sampleSize = 3;

        // Build the distinct operator.
        JavaRandomSampleOperator<Integer> sampleOperator =
                new JavaRandomSampleOperator<>(
                        sampleSize,
                        10,
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(inputStream)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};

        // Execute.
        evaluate(sampleOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideStream().collect(Collectors.toList());
        System.out.println(result);
        Assert.assertEquals(sampleSize, result.size());

    }
    @Test
    public void testUDFExecution() {
        // Prepare test data.
        Stream<Integer> inputStream = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).stream();


        // Build the distinct operator.
        JavaRandomSampleOperator<Integer> sampleOperator =
                new JavaRandomSampleOperator<>(
                        new myUDFSampleSize(),
                        10,
                        DataSetType.createDefaultUnchecked(Integer.class)
                );
        sampleOperator.setSeed(42);

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(inputStream)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};

        // Execute.
        this.evaluate(sampleOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideStream().collect(Collectors.toList());
        System.out.println(result);
        Assert.assertEquals(2, result.size());

    }

}

class myUDFSampleSize implements UDFSampleSize {

    int iteration;

    @Override
    public void open(ExecutionContext ctx) {
        this.iteration = ctx.getCurrentIteration();
    }

    @Override
    public int apply() {
        return iteration + 3; //given that the sample is not inside the loop, iteration = -1
    }
}
