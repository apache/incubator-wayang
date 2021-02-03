package org.apache.wayang.java.operators;

import org.apache.commons.lang3.Validate;
import org.junit.Test;
import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Test suite for {@link JavaObjectFileSink}.
 */
public class JavaObjectFileSinkTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testWritingDoesNotFail() throws IOException {
        // Prepare the sink.
        Path tempDir = Files.createTempDirectory("wayang-java");
        tempDir.toFile().deleteOnExit();
        Path targetFile = tempDir.resolve("testWritingDoesNotFail");
        final Stream<Integer> integerStream = enumerateRange(10000).stream();
        final JavaObjectFileSink<Integer> sink = new JavaObjectFileSink<>(
                targetFile.toUri().toString(),
                DataSetType.createDefault(Integer.class)
        );

        // Execute.
        ChannelInstance[] inputs = new ChannelInstance[]{createStreamChannelInstance(integerStream)};
        final ChannelInstance outputChannel = FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR
                .createChannel(null, configuration)
                .createInstance(null, null, -1);
        ChannelInstance[] outputs = new ChannelInstance[]{outputChannel};
        evaluate(sink, inputs, outputs);
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
