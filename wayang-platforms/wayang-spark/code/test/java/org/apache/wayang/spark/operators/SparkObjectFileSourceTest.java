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

package org.apache.wayang.spark.operators;

import org.apache.commons.lang3.Validate;
import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;

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
            final URL inputUrl = this.getClass().getResource("/0-to-10000.input");
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
