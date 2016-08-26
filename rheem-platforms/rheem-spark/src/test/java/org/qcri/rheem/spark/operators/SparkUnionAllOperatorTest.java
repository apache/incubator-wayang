package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkUnionAllOperator}.
 */
public class SparkUnionAllOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        RddChannel.Instance input0 = this.createRddChannelInstance(Arrays.asList(6, 0, 1, 1, 5, 2));
        RddChannel.Instance input1 = this.createRddChannelInstance(Arrays.asList(1, 1, 9));
        RddChannel.Instance output = this.createRddChannelInstance();

        // Build the UnionAll operator.
        SparkUnionAllOperator<Integer> unionAllOperator =
                new SparkUnionAllOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class)
                );


        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input0, input1};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(unionAllOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = output.<Integer>provideRdd().collect();
        Assert.assertEquals(9, result.size());
        Assert.assertEquals(Arrays.asList(6, 0, 1, 1, 5, 2, 1, 1, 9), result);

    }

}
