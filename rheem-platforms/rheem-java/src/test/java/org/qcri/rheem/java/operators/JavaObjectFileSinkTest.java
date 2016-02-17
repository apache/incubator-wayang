package org.qcri.rheem.java.operators;

import org.apache.commons.lang3.Validate;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.channels.TestChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

/**
 * Test suite for {@link JavaObjectFileSink}.
 */
public class JavaObjectFileSinkTest {

    @Test
    public void testWritingDoesNotFail() throws IOException {
        // Prepare the sink.
        Path tempDir = Files.createTempDirectory("rheem-java");
        tempDir.toFile().deleteOnExit();
        Path targetFile = tempDir.resolve("testWritingDoesNotFail");
        final Stream<Integer> integerStream = enumerateRange(10000).stream();
        final JavaObjectFileSink<Integer> sink = new JavaObjectFileSink<>(
                targetFile.toUri().toString(),
                DataSetType.createDefault(Integer.class)
        );

        // Execute.
        ChannelExecutor[] inputs = new ChannelExecutor[]{new TestChannelExecutor(integerStream)};
        ChannelExecutor[] outputs = new ChannelExecutor[]{};
        sink.evaluate(inputs, outputs, new FunctionCompiler());
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
