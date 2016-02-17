package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.channels.TestChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkJoinOperator}.
 */
public class SparkJoinOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        JavaRDD<Tuple2> inputRdd0 = this.getSC().parallelize(Arrays.asList(
                new Tuple2<>(1, "b"), new Tuple2<>(1, "c"), new Tuple2<>(2, "d"), new Tuple2<>(3, "e")));
        JavaRDD<Tuple2> inputRdd1 = this.getSC().parallelize(Arrays.asList(
                new Tuple2<>("x", 1), new Tuple2<>("y", 1), new Tuple2<>("z", 2), new Tuple2<>("w", 4)));

        // Build the Cartesian operator.
        SparkJoinOperator<Tuple2, Tuple2, Integer> join =
                new SparkJoinOperator<>(
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasic(Integer.class),
                                "field0"),
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasic(String.class),
                                "field1"));

        // Set up the ChannelExecutors.
        final ChannelExecutor[] inputs = new ChannelExecutor[]{
                new TestChannelExecutor(inputRdd0),
                new TestChannelExecutor(inputRdd1)
        };
        final ChannelExecutor[] outputs = new ChannelExecutor[]{
                new TestChannelExecutor()
        };

        // Execute.
        join.evaluate(inputs, outputs, new FunctionCompiler(), this.sparkExecutor);

        // Verify the outcome.
        final List<Tuple2<Tuple2<Integer, String>, Tuple2<String, Integer>>> result =
                outputs[0].<Tuple2<Tuple2<Integer, String>, Tuple2<String, Integer>>>provideRdd().collect();
        Assert.assertEquals(5, result.size());
        Assert.assertEquals(result.get(0), new Tuple2<>(new Tuple2<>(1, "b"), new Tuple2<>("x", 1)));
        Assert.assertEquals(result.get(1), new Tuple2<>(new Tuple2<>(1, "b"), new Tuple2<>("y", 1)));
        Assert.assertEquals(result.get(2), new Tuple2<>(new Tuple2<>(1, "c"), new Tuple2<>("x", 1)));
        Assert.assertEquals(result.get(3), new Tuple2<>(new Tuple2<>(1, "c"), new Tuple2<>("y", 1)));
        Assert.assertEquals(result.get(4), new Tuple2<>(new Tuple2<>(2, "d"), new Tuple2<>("z", 2)));


    }

}
