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

import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.channels.JavaChannelInstance;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Test suite for {@link JavaLocalCallbackSink}.
 */
public class JavaLocalCallbackSinkTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        List<Integer> inputValues = Arrays.asList(1, 2, 3);

        // Build the sink.
        List<Integer> collector = new LinkedList<>();
        JavaLocalCallbackSink<Integer> sink = new JavaLocalCallbackSink<>(collector::add, DataSetType.createDefault(Integer.class));

        // Execute.
        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createCollectionChannelInstance(inputValues)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{};
        evaluate(sink, inputs, outputs);

        // Verify the outcome.
        Assert.assertEquals(collector, inputValues);
    }
}
