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
 * Test suite for {@link SparkUnionAllOperator}.
 */
public class SparkUnionAllOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        JavaRDD<Integer> inputRdd0 = this.getSC().parallelize(Arrays.asList(6, 0, 1, 1, 5, 2));
        JavaRDD<Integer> inputRdd1 = this.getSC().parallelize(Arrays.asList(1, 1, 9));

        // Build the UnionAll operator.
        SparkUnionAllOperator<Integer> unionAllOperator =
                new SparkUnionAllOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class)
                );



        // Set up the ChannelExecutors.
        final ChannelExecutor[] inputs = new ChannelExecutor[]{
                new TestChannelExecutor(inputRdd0),
                new TestChannelExecutor(inputRdd1)
        };
        final ChannelExecutor[] outputs = new ChannelExecutor[]{
                new TestChannelExecutor()
        };

        // Execute.
        unionAllOperator.evaluate(inputs, outputs, new FunctionCompiler(), this.sparkExecutor);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideRdd().collect();
        Assert.assertEquals(9, result.size());
        Assert.assertEquals(Arrays.asList(6, 0, 1, 1, 5, 2, 1, 1, 9), result);

    }

}
