package io.rheem.rheem.spark.operators;

import org.apache.commons.lang3.Validate;
import org.junit.Test;
import io.rheem.rheem.basic.channels.FileChannel;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.spark.channels.RddChannel;
import io.rheem.rheem.spark.execution.SparkExecutor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Test suite for {@link SparkObjectFileSink}.
 */
public class SparkObjectFileSinkTest extends SparkOperatorTestBase {

    @Test
    public void testWritingDoesNotFail() throws IOException {
        SparkExecutor sparkExecutor = null;
        try {

            // Prepare the sink.
            Path tempDir = Files.createTempDirectory("rheem-spark");
            tempDir.toFile().deleteOnExit();
            Path targetFile = tempDir.resolve("testWritingDoesNotFail");
            RddChannel.Instance input = this.createRddChannelInstance(enumerateRange(10000));
            final SparkObjectFileSink<Integer> sink = new SparkObjectFileSink<>(
                    targetFile.toUri().toString(),
                    DataSetType.createDefault(Integer.class)
            );

            // Set up the ChannelInstances.
            final ChannelInstance[] inputs = new ChannelInstance[]{input};
            final ChannelInstance outputChannel = FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR
                    .createChannel(null, configuration)
                    .createInstance(sparkExecutor, null, -1);
            final ChannelInstance[] outputs = new ChannelInstance[]{outputChannel};

            // Execute.
            this.evaluate(sink, inputs, outputs);
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
