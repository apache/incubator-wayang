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
import org.apache.wayang.basic.operators.ParquetSink;
import org.apache.wayang.core.platform.ChannelInstance;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SparkParquetSinkTest extends SparkOperatorTestBase {

    @Test
    void writesDatasetToParquet() throws IOException {
        Dataset<Row> dataset = DatasetTestUtils.createSampleDataset(this.sparkExecutor);
        Path outputDir = Files.createTempDirectory("wayang-dataset-parquet-sink");
        try {
            SparkParquetSink sink = new SparkParquetSink(new ParquetSink(outputDir.toString(), true, true));

            ChannelInstance[] inputs = new ChannelInstance[]{this.createDatasetChannelInstance(dataset)};
            ChannelInstance[] outputs = new ChannelInstance[0];

            this.evaluate(sink, inputs, outputs);

            Dataset<Row> stored = this.sparkExecutor.ss.read().parquet(outputDir.toString());
            assertEquals(dataset.collectAsList(), stored.collectAsList());
        } finally {
            DatasetTestUtils.deleteRecursively(outputDir);
        }
    }

    @Test
    void writesRddToParquet() throws IOException {
        Path outputDir = Files.createTempDirectory("wayang-rdd-parquet-sink");
        try {
            SparkParquetSink sink = new SparkParquetSink(new ParquetSink(outputDir.toString(), true, false));
            ChannelInstance[] inputs = new ChannelInstance[]{this.createRddChannelInstance(DatasetTestUtils.sampleRecords())};
            ChannelInstance[] outputs = new ChannelInstance[0];

            this.evaluate(sink, inputs, outputs);

            Dataset<Row> stored = this.sparkExecutor.ss.read().parquet(outputDir.toString());
            assertEquals(DatasetTestUtils.sampleRows(), stored.collectAsList());
        } finally {
            DatasetTestUtils.deleteRecursively(outputDir);
        }
    }
}
