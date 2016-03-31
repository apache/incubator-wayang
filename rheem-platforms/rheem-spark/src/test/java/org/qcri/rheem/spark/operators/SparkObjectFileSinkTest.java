package org.qcri.rheem.spark.operators;

import org.apache.commons.lang3.Validate;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.channels.TestChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * Test suite for {@link SparkObjectFileSink}.
 */
public class SparkObjectFileSinkTest extends SparkOperatorTestBase {

    @Test
    public void testWritingDoesNotFail() throws IOException {
        SparkExecutor sparkExecutor = null;
        try {
            // Prepare Spark.
            final SparkPlatform sparkPlatform = SparkPlatform.getInstance();
            sparkExecutor = (SparkExecutor) sparkPlatform.createExecutor(Configuration.getDefaultConfiguration());
            final JavaSparkContext sc = sparkExecutor.sc;

            // Prepare the sink.
            Path tempDir = Files.createTempDirectory("rheem-spark");
            tempDir.toFile().deleteOnExit();
            Path targetFile = tempDir.resolve("testWritingDoesNotFail");
            final JavaRDD<Integer> integerRDD = sc.parallelize(enumerateRange(10000));
            final SparkObjectFileSink<Integer> sink = new SparkObjectFileSink<>(
                    targetFile.toUri().toString(),
                    DataSetType.createDefault(Integer.class)
            );

            // Set up the ChannelExecutors.
            final ChannelExecutor[] inputs = new ChannelExecutor[]{
                    new TestChannelExecutor(integerRDD)
            };
            final ChannelExecutor[] outputs = new ChannelExecutor[]{
            };

            // Execute.
            sink.evaluate(inputs, outputs, new FunctionCompiler(), this.sparkExecutor);
        } finally {
            if (sparkExecutor != null) sparkExecutor.dispose();
        }

    }

    static List<Integer> enumerateRange(int to) {
        Validate.isTrue(to >= 0);
        List<Integer> range = new ArrayList<>(to);
        for (int i = 0; i < to; i++) {
            range.add(i);
        }
        return range;
    }
}
