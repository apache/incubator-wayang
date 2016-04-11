package org.qcri.rheem.spark.operators;

import org.apache.commons.lang3.Validate;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.channels.TestChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

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
            // Prepare Spark.
            final SparkPlatform sparkPlatform = SparkPlatform.getInstance();
            sparkExecutor = (SparkExecutor) sparkPlatform.createExecutor(this.mockJob());

            // Prepare the source.
            final URL inputUrl = this.getClass().getResource("/0-to-10000.sequence_file");
            SparkObjectFileSource<Integer> source = new SparkObjectFileSource<>(
                    inputUrl.toString(), DataSetType.createDefault(Integer.class));

            // Set up the ChannelExecutors.
            final ChannelExecutor[] inputs = new ChannelExecutor[]{
            };
            final ChannelExecutor[] outputs = new ChannelExecutor[]{
                    new TestChannelExecutor()
            };

            // Execute.
            source.evaluate(inputs, outputs, new FunctionCompiler(), this.sparkExecutor);

            // Verify.
            Set<Integer> expectedValues = new HashSet<>(SparkObjectFileSourceTest.enumerateRange(10000));
            final List<Integer> rddList = outputs[0].<Integer>provideRdd().collect();
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
