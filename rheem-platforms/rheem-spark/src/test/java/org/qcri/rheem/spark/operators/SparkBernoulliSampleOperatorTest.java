package org.qcri.rheem.spark.operators;

import org.junit.Test;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkBernoulliSampleOperator}.
 */
public class SparkBernoulliSampleOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        List<Integer> inputData = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Build the distinct operator.
        SparkBernoulliSampleOperator<Integer> sampleOperator =
                new SparkBernoulliSampleOperator<>(
                        5,
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{this.createRddChannelInstance(inputData)};
        final ChannelInstance[] outputs = new ChannelInstance[]{this.createRddChannelInstance()};

        // Execute.
        this.evaluate(sampleOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = ((RddChannel.Instance) outputs[0]).<Integer>provideRdd().collect();
        System.out.println(result);
//        Assert.assertEquals(5, result.sampleSize()); //cannot check this, because the sample size returned is not always exact

    }

}
