package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.Collection;


/**
 * Test suite for {@link SparkCountOperator}.
 */
public class SparkCountOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        RddChannel.Instance input = this.createRddChannelInstance(Arrays.asList(1, 2, 3, 4, 5));
        CollectionChannel.Instance output = this.createCollectionChannelInstance();

        // Build the count operator.
        SparkCountOperator<Integer> countOperator =
                new SparkCountOperator<>(DataSetType.createDefaultUnchecked(Integer.class));

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(countOperator, inputs, outputs);

        // Verify the outcome.
        final Collection<Integer> result = output.provideCollection();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(Long.valueOf(5), result.iterator().next());

    }

}
