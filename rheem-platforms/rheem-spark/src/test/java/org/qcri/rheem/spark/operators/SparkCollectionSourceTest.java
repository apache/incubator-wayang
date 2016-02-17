package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.channels.TestChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test suite for the {@link SparkCollectionSource}.
 */
public class SparkCollectionSourceTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        Set<Integer> inputValues = new HashSet<>(Arrays.asList(1, 2, 3));
        SparkCollectionSource<Integer> collectionSource = new SparkCollectionSource<>(
                inputValues,
                DataSetType.createDefault(Integer.class));

        // Set up the ChannelExecutors.
        final ChannelExecutor[] inputs = new ChannelExecutor[]{};
        final ChannelExecutor[] outputs = new ChannelExecutor[]{
                new TestChannelExecutor()
        };

        // Execute.
        collectionSource.evaluate(inputs, outputs, new FunctionCompiler(), this.sparkExecutor);

        final Set<Integer> outputValues = new HashSet<>(outputs[0].<Integer>provideRdd().collect());
        Assert.assertEquals(outputValues, inputValues);
    }
}
