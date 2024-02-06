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

package org.apache.wayang.flink.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.apache.wayang.java.channels.CollectionChannel;
import org.junit.Assert;
import org.junit.Test;

import javax.xml.crypto.Data;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Test suite for {@link FlinkCountOperator}.
 */
public class FlinkCountOperatorTest extends FlinkOperatorTestBase{
    @Test
    public void testExecution() throws Exception {
        // Prepare test data.
        DataSetChannel.Instance input = this.createDataSetChannelInstance(Arrays.asList(1, 2, 3, 4, 5));
        DataSetChannel.Instance output = this.createDataSetChannelInstance();

        // Build the count operator.
        FlinkCountOperator<Integer> countOperator =
                new FlinkCountOperator<Integer>(DataSetType.createDefaultUnchecked(Integer.class));

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(countOperator, inputs, outputs);

        // Verify the outcome.
        final List<Object> result = output.provideDataSet().collect();
        Assert.assertEquals(1,result.size());
        Assert.assertEquals(Long.valueOf(5),result.iterator().next());

    }

}
