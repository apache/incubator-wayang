package org.qcri.rheem.spark.operators;

import org.apache.commons.lang3.Validate;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test suite for {@link SparkObjectFileSource}.
 */
public class SparkObjectFileSourceTest extends SparkOperatorTestBase {

    @Test
    public void testWritingDoesNotFail() throws IOException {
        SparkExecutor sparkExecutor = null;
        try {

            // Prepare the source.
            final URL inputUrl = this.getClass().getResource("/0-to-10000.sequence_file");
            SparkObjectFileSource<Integer> source = new SparkObjectFileSource<>(
                    inputUrl.toString(), DataSetType.createDefault(Integer.class));

            // Set up the ChannelInstances.
            final ChannelInstance[] inputs = new ChannelInstance[]{};
            final RddChannel.Instance output = this.createRddChannelInstance();
            final ChannelInstance[] outputs = new ChannelInstance[]{output};

            // Execute.
            this.evaluate(source, inputs, outputs);

            // Verify.
            Set<Integer> expectedValues = new HashSet<>(SparkObjectFileSourceTest.enumerateRange(10000));
            final List<Integer> rddList = output.<Integer>provideRdd().collect();
            for (Integer rddValue : rddList) {
                Assert.assertTrue("Value: " + rddValue, expectedValues.remove(rddValue));
            }
            Assert.assertEquals(0, expectedValues.size());
        } finally {
            if (sparkExecutor != null) sparkExecutor.dispose();
        }

    }

    private static List<Integer> enumerateRange(int to) {
        Validate.isTrue(to >= 0);
        List<Integer> range = new ArrayList<>(to);
        for (int i = 0; i < to; i++) {
            range.add(i);
        }
        return range;
    }
}
