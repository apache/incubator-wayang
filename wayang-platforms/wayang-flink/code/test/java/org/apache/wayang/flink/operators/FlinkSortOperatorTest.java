package org.apache.wayang.flink.operators;

import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link FlinkSortOperator}.
 */
public class FlinkSortOperatorTest extends FlinkOperatorTestBase {

    @Test
    public void testExecution() throws Exception {
        // Prepare test data.
        DataSetChannel.Instance input = this.createDataSetChannelInstance(Arrays.asList(6, 0, 1, 1, 5, 2));
        DataSetChannel.Instance output = this.createDataSetChannelInstance();


        // Build the sort operator.
        FlinkSortOperator<Integer, Integer> sortOperator =
                new FlinkSortOperator<>(
                        new TransformationDescriptor(r->r, Integer.class, Integer.class),
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(sortOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = output.<Integer>provideDataSet().collect();
        Assert.assertEquals(6, result.size());
        Assert.assertEquals(Arrays.asList(0, 1, 1, 2, 5, 6), result);

    }

}
