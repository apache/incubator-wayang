package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkFilterOperator}.
 */
public class SparkFlatMapOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        RddChannel.Instance input = this.createRddChannelInstance(Arrays.asList("one phrase", "two sentences", "three lines"));
        RddChannel.Instance output = this.createRddChannelInstance();

        SparkFlatMapOperator<String, String> flatMapOperator = new SparkFlatMapOperator<>(
                DataSetType.createDefaultUnchecked(String.class),
                DataSetType.createDefaultUnchecked(String.class),
                new FlatMapDescriptor<>(phrase -> Arrays.asList(phrase.split(" ")), String.class, String.class)
        );

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{input};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(flatMapOperator, inputs, outputs);

        // Verify the outcome.
        final List<String> result = output.<String>provideRdd().collect();
        Assert.assertEquals(6, result.size());
        Assert.assertEquals(Arrays.asList("one", "phrase", "two", "sentences", "three", "lines"), result);

    }

}
