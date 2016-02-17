package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.channels.TestChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkCartesianOperator}.
 */
public class SparkCartesianOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        JavaRDD<Integer> inputRdd0 = this.getSC().parallelize(Arrays.asList(1, 2));
        JavaRDD<String> inputRdd1 = this.getSC().parallelize(Arrays.asList("a", "b", "c"));

        // Build the Cartesian operator.
        SparkCartesianOperator<Integer, String> cartesianOperator =
                new SparkCartesianOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class),
                        DataSetType.createDefaultUnchecked(String.class));

        // Set up the ChannelExecutors.
        final ChannelExecutor[] inputs = new ChannelExecutor[]{
                new TestChannelExecutor(inputRdd0),
                new TestChannelExecutor(inputRdd1)
        };
        final ChannelExecutor[] outputs = new ChannelExecutor[]{
                new TestChannelExecutor()
        };

        // Execute.
        cartesianOperator.evaluate(inputs, outputs, new FunctionCompiler(), this.sparkExecutor);

        // Verify the outcome.
        final List<Tuple2<Integer, String>> result = outputs[0].<Tuple2<Integer, String>>provideRdd().collect();
        Assert.assertEquals(6, result.size());
        Assert.assertEquals(result.get(0), new Tuple2(1, "a"));

    }

}
