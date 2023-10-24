package org.apache.wayang.spark.operators;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.operators.ml.SparkKMeansOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SparkKMeansOperatorTest extends SparkOperatorTestBase {
    @Test
    public void testExecution() {
        // Prepare test data.
        RddChannel.Instance input = this.createRddChannelInstance(Arrays.asList(
                new double[]{1, 2, 3},
                new double[]{-1, -2, -3},
                new double[]{2, 4, 6}));
        RddChannel.Instance output = this.createRddChannelInstance();

        SparkKMeansOperator kMeansOperator = new SparkKMeansOperator(2);

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{input};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(kMeansOperator, inputs, outputs);

        // Verify the outcome.
        final List<Tuple2<double[], Integer>> results = output.<Tuple2<double[], Integer>>provideRdd().collect();
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(
                results.get(0).field1,
                results.get(2).field1
        );
    }
}
