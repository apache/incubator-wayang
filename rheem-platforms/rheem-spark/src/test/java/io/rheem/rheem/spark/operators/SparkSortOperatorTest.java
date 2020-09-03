package io.rheem.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import io.rheem.rheem.core.function.TransformationDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkSortOperator}.
 */
public class SparkSortOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        RddChannel.Instance input = this.createRddChannelInstance(Arrays.asList(6, 0, 1, 1, 5, 2));
        RddChannel.Instance output = this.createRddChannelInstance();


        // Build the sort operator.
        SparkSortOperator<Integer, Integer> sortOperator =
                new SparkSortOperator<>(
                        new TransformationDescriptor(r->r, Integer.class, Integer.class),
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(sortOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = output.<Integer>provideRdd().collect();
        Assert.assertEquals(6, result.size());
        Assert.assertEquals(Arrays.asList(0, 1, 1, 2, 5, 6), result);

    }

}
