package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.JavaChannelInstance;

import java.util.Arrays;
import java.util.Collection;
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
        Collection<Integer> inputCollection = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        final int sampleSize = 3;

        // Build the distinct operator.
        JavaRandomSampleOperator<Integer> sampleOperator =
                new JavaRandomSampleOperator<>(
                        iteration -> sampleSize,
                        DataSetType.createDefaultUnchecked(Integer.class),
                        iteration -> 42L
                );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createCollectionChannelInstance(inputCollection)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};

        // Execute.
        evaluate(sampleOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideStream().collect(Collectors.toList());
        Assert.assertEquals(sampleSize, result.size());

    }

    @Test
    public void testUDFExecution() {
        // Prepare test data.
        Stream<Integer> inputStream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Build the distinct operator.
        JavaRandomSampleOperator<Integer> sampleOperator =
                new JavaRandomSampleOperator<>(
                        iterationNumber -> iterationNumber + 3, // iterationNumber=-1, hence sampleSize=2
                        DataSetType.createDefaultUnchecked(Integer.class),
                        iteration -> 42L
                );
        sampleOperator.setDatasetSize(10);

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(inputStream)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};

        // Execute.
        evaluate(sampleOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideStream().collect(Collectors.toList());
        Assert.assertEquals(2, result.size());

    }

    @Test
    public void testLargerSampleExecution() {
        // Prepare test data.
        Stream<Integer> inputStream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Build the distinct operator.
        JavaRandomSampleOperator<Integer> sampleOperator =
                new JavaRandomSampleOperator<>(
                        iterationNumber -> 15, // sample size larger than dataset size
                        DataSetType.createDefaultUnchecked(Integer.class),
                        iteration -> 42L
                );
        sampleOperator.setDatasetSize(10);

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(inputStream)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};

        // Execute.
        evaluate(sampleOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideStream().collect(Collectors.toList());
        Assert.assertEquals(10, result.size());

    }

}