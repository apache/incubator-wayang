package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkFilterOperator}.
 */
public class SparkMapPartitionsOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        RddChannel.Instance input = this.createRddChannelInstance(Arrays.asList(0, 1, 1, 2, 6));
        RddChannel.Instance output = this.createRddChannelInstance();

        // Create the mapPartitions operator.
        SparkMapPartitionsOperator<Integer, Integer> mapPartitionsOperator =
                new SparkMapPartitionsOperator<>(
                        new TransformationDescriptor<>(item -> item + 1, Integer.class, Integer.class)
                );

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{input};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(mapPartitionsOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = output.<Integer>provideRdd().collect();
        Assert.assertEquals(5, result.size());
        Assert.assertEquals(Arrays.asList(1, 2, 2, 3, 7), result);

    }

}
