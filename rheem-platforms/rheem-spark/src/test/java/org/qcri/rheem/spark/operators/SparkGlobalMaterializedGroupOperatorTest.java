package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Test suite for {@link SparkGlobalMaterializedGroupOperator}.
 */
public class SparkGlobalMaterializedGroupOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        Collection<Integer> inputCollection = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Build the reduce operator.
        SparkGlobalMaterializedGroupOperator<Integer> globalGroup =
                new SparkGlobalMaterializedGroupOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class),
                        DataSetType.createGroupedUnchecked(Iterable.class)
                );

        // Execute.
        ChannelInstance[] inputs = new RddChannel.Instance[]{this.createRddChannelInstance(inputCollection)};
        ChannelInstance[] outputs = new RddChannel.Instance[]{this.createRddChannelInstance()};
        this.evaluate(globalGroup, inputs, outputs);

        // Verify the outcome.
        final Collection<Iterable<Integer>> result = ((RddChannel.Instance) outputs[0]).<Iterable<Integer>>provideRdd().collect();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(inputCollection, result.iterator().next());

    }

    @Test
    public void testExecutionWithoutData() {
        // Prepare test data.
        Collection<Integer> inputCollection = Collections.emptyList();

        // Build the reduce operator.
        SparkGlobalMaterializedGroupOperator<Integer> globalGroup =
                new SparkGlobalMaterializedGroupOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class),
                        DataSetType.createGroupedUnchecked(Iterable.class)
                );

        // Execute.
        ChannelInstance[] inputs = new RddChannel.Instance[]{this.createRddChannelInstance(inputCollection)};
        ChannelInstance[] outputs = new RddChannel.Instance[]{this.createRddChannelInstance()};
        this.evaluate(globalGroup, inputs, outputs);

        // Verify the outcome.
        final Collection<Iterable<Integer>> result = ((RddChannel.Instance) outputs[0]).<Iterable<Integer>>provideRdd().collect();
        Assert.assertEquals(1, result.size());
        Assert.assertFalse(RheemCollections.getSingle(result).iterator().hasNext());
    }
}
