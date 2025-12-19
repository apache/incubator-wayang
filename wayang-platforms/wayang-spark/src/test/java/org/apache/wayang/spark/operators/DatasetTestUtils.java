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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.spark.execution.SparkExecutor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Comparator;

final class DatasetTestUtils {

    private DatasetTestUtils() {
    }

    static Dataset<Row> createSampleDataset(SparkExecutor sparkExecutor) {
        return sparkExecutor.ss.createDataFrame(sampleRows(), sampleSchema());
    }

    static List<Row> sampleRows() {
        return Arrays.asList(
                RowFactory.create("alice", 30),
                RowFactory.create("bob", 25),
                RowFactory.create("carol", 41)
        );
    }

    static List<Record> sampleRecords() {
        return Arrays.asList(
                new Record("alice", 30),
                new Record("bob", 25),
                new Record("carol", 41)
        );
    }

    static StructType sampleSchema() {
        StructField[] fields = new StructField[]{
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false)
        };
        return new StructType(fields);
    }

    static void deleteRecursively(Path directory) throws IOException {
        if (Files.notExists(directory)) {
            return;
        }
        Files.walk(directory)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
