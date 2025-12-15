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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.spark.channels.RddChannel;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SparkDatasetToRddOperatorTest extends SparkOperatorTestBase {

    @Test
    void testConversionPreservesRows() {
        Dataset<Row> dataset = DatasetTestUtils.createSampleDataset(this.sparkExecutor);
        SparkDatasetToRddOperator operator = new SparkDatasetToRddOperator();

        ChannelInstance[] inputs = new ChannelInstance[]{this.createDatasetChannelInstance(dataset)};
        ChannelInstance[] outputs = new ChannelInstance[]{this.createRddChannelInstance()};

        this.evaluate(operator, inputs, outputs);

        List<Row> rows = ((RddChannel.Instance) outputs[0]).<Row>provideRdd().collect();
        assertEquals(dataset.collectAsList(), rows);
    }
}
