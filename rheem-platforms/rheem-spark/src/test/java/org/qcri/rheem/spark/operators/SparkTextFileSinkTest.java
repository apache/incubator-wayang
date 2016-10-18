package org.qcri.rheem.spark.operators;

import org.junit.Test;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.spark.channels.RddChannel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Test suite for {@link SparkTextFileSink}.
 */
public class SparkTextFileSinkTest extends SparkOperatorTestBase {

    @Test
    public void testWritingDoesNotFail() throws IOException {
        // Prepare the sink.
        Path tempDir = Files.createTempDirectory("rheem-spark");
        tempDir.toFile().deleteOnExit();
        Path targetFile = tempDir.resolve("testWritingDoesNotFail");
        RddChannel.Instance input = this.createRddChannelInstance(Arrays.asList(1.123f, -0.1f, 3f));
        final SparkTextFileSink<Float> sink = new SparkTextFileSink<>(
                targetFile.toUri().toString(),
                new TransformationDescriptor<>(
                        f -> String.format("%.2f", f),
                        Float.class, String.class
                )
        );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input};
        final ChannelInstance[] outputs = new ChannelInstance[]{};

        // Execute.
        this.evaluate(sink, inputs, outputs);
    }

}
