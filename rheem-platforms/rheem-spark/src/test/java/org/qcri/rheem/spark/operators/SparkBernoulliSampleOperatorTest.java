package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.channels.TestChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkBernoulliSampleOperator}.
 */
public class SparkBernoulliSampleOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        JavaRDD<Integer> inputStream = this.getSC().parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        // Build the distinct operator.
        SparkBernoulliSampleOperator<Integer> sampleOperator =
                new SparkBernoulliSampleOperator<>(
                        0.5,
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Set up the ChannelExecutors.
        final ChannelExecutor[] inputs = new ChannelExecutor[]{
                new TestChannelExecutor(inputStream)
        };
        final ChannelExecutor[] outputs = new ChannelExecutor[]{
                new TestChannelExecutor()
        };

        // Execute.
        sampleOperator.evaluate(inputs, outputs, new FunctionCompiler(), this.sparkExecutor);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideRdd().collect();
        System.out.println(result);
//        Assert.assertEquals(5, result.size());

    }

}
