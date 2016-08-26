package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkCartesianOperator}.
 */
public class SparkCartesianOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        RddChannel.Instance input0 = this.createRddChannelInstance(Arrays.asList(1, 2));
        RddChannel.Instance input1 = this.createRddChannelInstance(Arrays.asList("a", "b", "c"));
        RddChannel.Instance output = this.createRddChannelInstance();

        // Build the Cartesian operator.
        SparkCartesianOperator<Integer, String> cartesianOperator =
                new SparkCartesianOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class),
                        DataSetType.createDefaultUnchecked(String.class));

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input0, input1};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(cartesianOperator, inputs, outputs);

        // Verify the outcome.
        final List<Tuple2<Integer, String>> result = output.<Tuple2<Integer, String>>provideRdd().collect();
        Assert.assertEquals(6, result.size());
        Assert.assertEquals(result.get(0), new Tuple2(1, "a"));

    }

}
