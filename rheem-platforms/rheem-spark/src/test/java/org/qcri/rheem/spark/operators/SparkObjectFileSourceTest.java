package org.qcri.rheem.spark.operators;

import org.apache.commons.lang3.Validate;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.io.IOException;
import java.net.URL;
import java.util.*;

import static org.mockito.Mockito.mock;

/**
 * Test suite for {@link SparkObjectFileSource}.
 */
public class SparkObjectFileSourceTest {

    @Test
    public void testWritingDoesNotFail() throws IOException {
        SparkExecutor sparkExecutor = null;
        try {
            // Prepare Spark.
            final SparkPlatform sparkPlatform = SparkPlatform.getInstance();
            sparkExecutor = (SparkExecutor) sparkPlatform.createExecutor();

            // Prepare the source.
            final URL inputUrl = this.getClass().getResource("/0-to-10000.sequence_file");
            SparkObjectFileSource<Integer> source = new SparkObjectFileSource<>(
                    inputUrl.toString(), DataSetType.createDefault(Integer.class));

            // Execute.
            final JavaRDDLike[] outputRdds = source.evaluate(new JavaRDDLike[0], mock(FunctionCompiler.class), sparkExecutor);

            // Verify.
            Assert.assertTrue(outputRdds.length == 1);

            Set<Integer> expectedValues = new HashSet<>(SparkObjectFileSourceTest.enumerateRange(10000));
            final List<Integer> rddList = outputRdds[0].collect();
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
