package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkJoinOperator}.
 */
public class SparkJoinOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        RddChannel.Instance input0 = this.createRddChannelInstance(Arrays.asList(
                new Tuple2<>(1, "b"), new Tuple2<>(1, "c"), new Tuple2<>(2, "d"), new Tuple2<>(3, "e")));
        RddChannel.Instance input1 = this.createRddChannelInstance(Arrays.asList(
                new Tuple2<>("x", 1), new Tuple2<>("y", 1), new Tuple2<>("z", 2), new Tuple2<>("w", 4)));
        RddChannel.Instance output = this.createRddChannelInstance();

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
                                DataUnitType.createBasic(Integer.class),
                                "field1"));

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input0, input1};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(join, inputs, outputs);

        // Verify the outcome.
        final List<Tuple2<Tuple2<Integer, String>, Tuple2<String, Integer>>> result =
                output.<Tuple2<Tuple2<Integer, String>, Tuple2<String, Integer>>>provideRdd().collect();
        Assert.assertEquals(5, result.size());
        Assert.assertEquals(result.get(0), new Tuple2<>(new Tuple2<>(1, "b"), new Tuple2<>("x", 1)));
        Assert.assertEquals(result.get(1), new Tuple2<>(new Tuple2<>(1, "b"), new Tuple2<>("y", 1)));
        Assert.assertEquals(result.get(2), new Tuple2<>(new Tuple2<>(1, "c"), new Tuple2<>("x", 1)));
        Assert.assertEquals(result.get(3), new Tuple2<>(new Tuple2<>(1, "c"), new Tuple2<>("y", 1)));
        Assert.assertEquals(result.get(4), new Tuple2<>(new Tuple2<>(2, "d"), new Tuple2<>("z", 2)));


    }

}
