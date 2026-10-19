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

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test suite for {@link JavaParquetSource}.
 */
class JavaParquetSourceTest extends JavaExecutionOperatorTestBase {

    @Test
    void testReadAllColumns() {
            Path parquetFile = Paths.get("src/test/resources/data.parquet").toAbsolutePath();
            JavaParquetSource source = new JavaParquetSource(parquetFile.toUri().toString(), null);

            ChannelInstance[] inputs = new ChannelInstance[0];
            ChannelInstance[] outputs = new ChannelInstance[]{createStreamChannelInstance()};
            evaluate(source, inputs, outputs);

            List<Record> result = ((StreamChannel.Instance) outputs[0]).<Record>provideStream().toList();

            assertEquals(3, result.size());
            assertEquals("alice", result.get(0).getString(0));
            assertEquals(30, result.get(0).getInt(1));
            assertEquals("bob", result.get(1).getString(0));
            assertEquals(25, result.get(1).getInt(1));
            assertEquals("carol", result.get(2).getString(0));
            assertEquals(41, result.get(2).getInt(1));
    }

    @Test
    void testReadWithProjection() {
            Path parquetFile = Paths.get("src/test/resources/data.parquet").toAbsolutePath();
            JavaParquetSource source = new JavaParquetSource(parquetFile.toUri().toString(), new String[]{"name"});

            ChannelInstance[] inputs = new ChannelInstance[0];
            ChannelInstance[] outputs = new ChannelInstance[]{createStreamChannelInstance()};
            evaluate(source, inputs, outputs);

            List<Record> result = ((StreamChannel.Instance) outputs[0]).<Record>provideStream().toList();

            assertEquals(3, result.size());
            // Each projected record contains only the "name" column
            assertEquals(1, result.get(0).size());
            assertEquals("alice", result.get(0).getString(0));
            assertEquals("bob", result.get(1).getString(0));
            assertEquals("carol", result.get(2).getString(0));
    }

    /* The following lines were used to create the sample Parquet file.
    We keep it here for reference, but we don't want to run it in the test suite as it adds a dependency on Hadoop and Parquet libraries and complicates the test setup.
    */

    /*
    private static final Schema SCHEMA = SchemaBuilder.record("TestRecord")
            .namespace("org.apache.wayang.test")
            .fields()
            .requiredString("name")
            .requiredInt("age")
            .endRecord();

    private static Path writeSampleParquet(Path dir) throws IOException {
        Path file = dir.resolve("data.parquet");
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toUri());
        List<GenericRecord> records = Arrays.asList(
                makeRecord("alice", 30),
                makeRecord("bob", 25),
                makeRecord("carol", 41)
        );
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
                .withSchema(SCHEMA)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()) {
            for (GenericRecord record : records) {
                writer.write(record);
            }
        }
        return file;
    }

    private static GenericRecord makeRecord(String name, int age) {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("name", name);
        record.put("age", age);
        return record;
    }
    */
}
