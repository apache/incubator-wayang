package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.channels.TestChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkSortOperator}.
 */
public class SparkSortOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        JavaRDD<Integer> inputRdd = this.getSC().parallelize(Arrays.asList(6, 0, 1, 1, 5, 2));

        // Build the sort operator.
        SparkSortOperator<Integer> sortOperator =
                new SparkSortOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Set up the ChannelExecutors.
        final ChannelExecutor[] inputs = new ChannelExecutor[]{
                new TestChannelExecutor(inputRdd)
        };
        final ChannelExecutor[] outputs = new ChannelExecutor[]{
                new TestChannelExecutor()
        };

        // Execute.
        sortOperator.evaluate(inputs, outputs, new FunctionCompiler(), this.sparkExecutor);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideRdd().collect();
        Assert.assertEquals(6, result.size());
        Assert.assertEquals(Arrays.asList(0, 1, 1, 2, 5, 6), result);

    }

}
