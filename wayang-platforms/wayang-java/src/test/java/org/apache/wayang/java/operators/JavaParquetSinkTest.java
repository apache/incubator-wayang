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

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.ParquetSink;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test suite for {@link JavaParquetSink}.
 */
class JavaParquetSinkTest extends JavaExecutionOperatorTestBase {

    @Test
    void testWriteStringRecords() throws IOException {
        List<Record> records = Arrays.asList(
                new Record("a", "hello"),
                new Record("b", "world"),
                new Record("c", "test")
        );

        java.nio.file.Path tempDir = Files.createTempDirectory("wayang-java-parquet-sink");
        java.nio.file.Path outputFile = tempDir.resolve("test-strings.parquet");

        try {
            JavaParquetSink sink = new JavaParquetSink(
                    new ParquetSink(outputFile.toUri().toString(), true, false)
            );

            ChannelInstance[] inputs = new ChannelInstance[]{
                    createCollectionChannelInstance(records)
            };
            evaluate(sink, inputs, new ChannelInstance[0]);

            List<GenericRecord> readBack = readParquetFile(outputFile.toString());
            assertEquals(3, readBack.size());
            assertEquals("a", readBack.get(0).get("field0").toString());
            assertEquals("hello", readBack.get(0).get("field1").toString());
            assertEquals("b", readBack.get(1).get("field0").toString());
            assertEquals("world", readBack.get(1).get("field1").toString());
            assertEquals("c", readBack.get(2).get("field0").toString());
            assertEquals("test", readBack.get(2).get("field1").toString());
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    void testWriteMixedTypeRecords() throws IOException {
        List<Record> records = Arrays.asList(
                new Record(1, "alpha", 10.5),
                new Record(2, "beta", 20.0),
                new Record(3, "gamma", 30.5)
        );

        java.nio.file.Path tempDir = Files.createTempDirectory("wayang-java-parquet-sink");
        java.nio.file.Path outputFile = tempDir.resolve("test-mixed.parquet");

        try {
            JavaParquetSink sink = new JavaParquetSink(
                    new ParquetSink(outputFile.toUri().toString(), true, false)
            );

            ChannelInstance[] inputs = new ChannelInstance[]{
                    createCollectionChannelInstance(records)
            };
            evaluate(sink, inputs, new ChannelInstance[0]);

            List<GenericRecord> readBack = readParquetFile(outputFile.toString());
            assertEquals(3, readBack.size());
            assertEquals(1, readBack.get(0).get("field0"));
            assertEquals("alpha", readBack.get(0).get("field1").toString());
            assertEquals(10.5, readBack.get(0).get("field2"));
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    void testWriteWithRecordType() throws IOException {
        List<Record> records = Arrays.asList(
                new Record("x1", 100),
                new Record("x2", 200),
                new Record("x3", 300)
        );

        java.nio.file.Path tempDir = Files.createTempDirectory("wayang-java-parquet-sink");
        java.nio.file.Path outputFile = tempDir.resolve("test-recordtype.parquet");

        try {
            DataSetType<Record> typedDataSet = DataSetType.createDefault(
                    new RecordType("name", "value")
            );
            ParquetSink logicalSink = new ParquetSink(
                    outputFile.toUri().toString(), true, false, typedDataSet
            );
            JavaParquetSink sink = new JavaParquetSink(logicalSink);

            ChannelInstance[] inputs = new ChannelInstance[]{
                    createCollectionChannelInstance(records)
            };
            evaluate(sink, inputs, new ChannelInstance[0]);

            List<GenericRecord> readBack = readParquetFile(outputFile.toString());
            assertEquals(3, readBack.size());
            assertEquals("x1", readBack.get(0).get("name").toString());
            assertEquals(100, readBack.get(0).get("value"));
            assertEquals("x2", readBack.get(1).get("name").toString());
            assertEquals(200, readBack.get(1).get("value"));
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    void testOverwriteExistingFile() throws IOException {
        java.nio.file.Path tempDir = Files.createTempDirectory("wayang-java-parquet-sink");
        java.nio.file.Path outputFile = tempDir.resolve("test-overwrite.parquet");

        try {
            // First write
            List<Record> firstRecords = Arrays.asList(
                    new Record("old", "data")
            );
            JavaParquetSink sink1 = new JavaParquetSink(
                    new ParquetSink(outputFile.toUri().toString(), true, false)
            );
            evaluate(sink1,
                    new ChannelInstance[]{createCollectionChannelInstance(firstRecords)},
                    new ChannelInstance[0]);

            // Second write with overwrite
            List<Record> secondRecords = Arrays.asList(
                    new Record("new", "data"),
                    new Record("more", "records")
            );
            JavaParquetSink sink2 = new JavaParquetSink(
                    new ParquetSink(outputFile.toUri().toString(), true, false)
            );
            evaluate(sink2,
                    new ChannelInstance[]{createCollectionChannelInstance(secondRecords)},
                    new ChannelInstance[0]);

            // We check if only second write's data exists
            List<GenericRecord> readBack = readParquetFile(outputFile.toString());
            assertEquals(2, readBack.size());
            assertEquals("new", readBack.get(0).get("field0").toString());
            assertEquals("more", readBack.get(1).get("field0").toString());
        } finally {
            deleteRecursively(tempDir);
        }
    }

    private List<GenericRecord> readParquetFile(String path) throws IOException {
        List<GenericRecord> records = new ArrayList<>();
        Configuration conf = new Configuration();
        Path hadoopPath = new Path(path);
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(hadoopPath, conf)).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                records.add(record);
            }
        }
        return records;
    }

    private void deleteRecursively(java.nio.file.Path dir) throws IOException {
        if (Files.exists(dir)) {
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                        }
                    });
        }
    }
}