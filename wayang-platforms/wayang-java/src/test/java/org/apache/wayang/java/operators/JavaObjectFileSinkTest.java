/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    // If this test fails, make sure you have Hadoop installed and it's HADOOP_HOME is set.
    // Also, if on Windows: Make sure you installed the winutils binaries (https://github.com/cdarlint/winutils)
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
