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
 * Test suite for {@link SparkCountOperator}.
 */
public class SparkCountOperatorTest extends SparkOperatorTestBase{

    @Test
    public void testExecution() {
        // Prepare test data.
        JavaRDD<Integer> inputStream = this.getSC().parallelize(Arrays.asList(1, 2, 3, 4, 5));

        // Build the count operator.
        SparkCountOperator<Integer> countOperator =
                new SparkCountOperator<>(DataSetType.createDefaultUnchecked(Integer.class));

        // Set up the ChannelExecutors.
        final ChannelExecutor[] inputs = new ChannelExecutor[]{
                new TestChannelExecutor(inputStream)
        };
        final ChannelExecutor[] outputs = new ChannelExecutor[]{
                new TestChannelExecutor()
        };

        // Execute.
        countOperator.evaluate(inputs, outputs, new FunctionCompiler(), this.sparkExecutor);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideRdd().collect();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(Long.valueOf(5), result.get(0));

    }

}
