package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test suite for {@link SparkGlobalReduceOperator}.
 */
public class SparkGlobalReduceOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        RddChannel.Instance input = this.createRddChannelInstance(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        CollectionChannel.Instance output = this.createCollectionChannelInstance();

        // Build the reduce operator.
        SparkGlobalReduceOperator<Integer> globalReduce =
                new SparkGlobalReduceOperator<>(
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        new ReduceDescriptor<>(
                                (a, b) -> a + b, DataUnitType.createGrouped(Integer.class),
                                DataUnitType.createBasic(Integer.class)
                        )
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(globalReduce, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = RheemCollections.asList(output.provideCollection());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(Integer.valueOf((10 + 1) * (10 / 2)), result.get(0)); // Props to Gauss!

    }

    @Ignore("Spark cannot reduce empty collections.")
    @Test
    public void testExecutionWithoutData() {
        // Prepare test data.
        RddChannel.Instance input = this.createRddChannelInstance(Collections.emptyList());
        CollectionChannel.Instance output = this.createCollectionChannelInstance();

        // Build the reduce operator.
        SparkGlobalReduceOperator<Integer> globalReduce =
                new SparkGlobalReduceOperator<>(
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        new ReduceDescriptor<>(
                                (a, b) -> a + b, DataUnitType.createGrouped(Integer.class),
                                DataUnitType.createBasic(Integer.class)
                        )
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(globalReduce, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = RheemCollections.asList(output.provideCollection());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(Integer.valueOf(0), result.get(0));

    }
}
