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
import org.apache.spark.sql.SaveMode;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.spark.channels.DatasetChannel;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SparkParquetSourceDatasetOutputTest extends SparkOperatorTestBase {

    @Test
    void producesDatasetChannel() throws IOException {
        Dataset<Row> dataset = DatasetTestUtils.createSampleDataset(this.sparkExecutor);
        Path inputPath = Files.createTempDirectory("wayang-parquet-source");
        try {
            dataset.write().mode(SaveMode.Overwrite).parquet(inputPath.toString());

            SparkParquetSource source = new SparkParquetSource(inputPath.toString(), null);
            source.preferDatasetOutput(true);

            ChannelInstance[] inputs = new ChannelInstance[0];
            ChannelInstance[] outputs = new ChannelInstance[]{this.createDatasetChannelInstance()};

            this.evaluate(source, inputs, outputs);

            Dataset<Row> result = ((DatasetChannel.Instance) outputs[0]).provideDataset();
            assertEquals(dataset.collectAsList(), result.collectAsList());
        } finally {
            DatasetTestUtils.deleteRecursively(inputPath);
        }
    }
}
