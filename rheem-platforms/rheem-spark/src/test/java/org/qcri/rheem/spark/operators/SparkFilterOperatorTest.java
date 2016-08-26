package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkFilterOperator}.
 */
public class SparkFilterOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        RddChannel.Instance input = this.createRddChannelInstance(Arrays.asList(0, 1, 1, 2, 6));
        RddChannel.Instance output = this.createRddChannelInstance();

        // Build the distinct operator.
        SparkFilterOperator<Integer> filterOperator =
                new SparkFilterOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class),
                        new PredicateDescriptor<>(item -> (item > 0), Integer.class)
                );

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{input};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(filterOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = output.<Integer>provideRdd().collect();
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(Arrays.asList(1, 1, 2, 6), result);

    }

}
