package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkDistinctOperator}.
 */
public class SparkDistinctOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        List<Integer> inputData = Arrays.asList(0, 1, 1, 6, 2, 2, 6, 6);

        // Build the distinct operator.
        SparkDistinctOperator<Integer> distinctOperator =
                new SparkDistinctOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{this.createRddChannelInstance(inputData)};
        final ChannelInstance[] outputs = new ChannelInstance[]{this.createRddChannelInstance()};

        // Execute.
        this.evaluate(distinctOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = ((RddChannel.Instance) outputs[0]).<Integer>provideRdd().collect();
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(Arrays.asList(0, 1, 6, 2), result);

    }

}
