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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.ParquetSink;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Writes {@link Record}s to a Parquet file using the Java platform.
 */
public class JavaParquetSink extends ParquetSink implements JavaExecutionOperator {

    private static final int SCHEMA_SAMPLE_SIZE = 50;

    public JavaParquetSink(ParquetSink that) {
        super(that.getOutputUrl(), that.isOverwrite(), that.prefersDataset(), that.getType());
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == 1;
        assert outputs.length == 0;

        // Get the input stream and collect all records into a list
        final List<Record> records = this.getRecords(inputs[0]);

        if (records.isEmpty()) {
            return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
        }

        try {
            // Handle overwrite — delete existing file if needed
            Path outputPath = new Path(this.getOutputUrl());
            Configuration conf = new Configuration();
            if (this.isOverwrite()) {
                FileSystem fs = outputPath.getFileSystem(conf);
                fs.delete(outputPath, true);
            }

            // Infer schema from RecordType + sampled records
            Schema schema = this.inferSchema(records);

            // Write records as Parquet
            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputPath)
                    .withSchema(schema)
                    .withConf(conf)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build()) {

                for (Record record : records) {
                    writer.write(this.convertToGenericRecord(record, schema));
                }
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to write Parquet file: " + this.getOutputUrl(), e);
        }

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    /**
     * Extracts records from the input channel, handling both Stream and Collection channels.
     */
    private List<Record> getRecords(ChannelInstance input) {
        if (input instanceof CollectionChannel.Instance) {
            return ((CollectionChannel.Instance) input).<Record>provideCollection()
                    .stream().collect(Collectors.toList());
        }
        return ((StreamChannel.Instance) input).<Record>provideStream()
                .collect(Collectors.toList());
    }

    /**
     * Infers an Avro schema from the RecordType (if available) and sampled record values.
     */
    private Schema inferSchema(List<Record> records) {
        String[] fieldNames = this.resolveFieldNames(records);
        List<Record> samples = records.subList(0, Math.min(SCHEMA_SAMPLE_SIZE, records.size()));

        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder
                .record("WayangRecord")
                .namespace("org.apache.wayang")
                .fields();

        for (int i = 0; i < fieldNames.length; i++) {
            Schema.Type avroType = this.inferColumnType(samples, i);
            // Make fields nullable — union of [null, type]
            Schema fieldSchema = Schema.createUnion(
                    Schema.create(Schema.Type.NULL),
                    Schema.create(avroType)
            );
            fields.name(fieldNames[i]).type(fieldSchema).noDefault();
        }

        return fields.endRecord();
    }

    /**
     * Resolves field names from RecordType if available, otherwise generates field0, field1, etc.
     */
    private String[] resolveFieldNames(List<Record> records) {
        DataSetType<Record> dataSetType = this.getType();
        if (dataSetType != null && dataSetType.getDataUnitType() instanceof RecordType) {
            RecordType recordType = (RecordType) dataSetType.getDataUnitType();
            if (recordType.getFieldNames() != null && recordType.getFieldNames().length > 0) {
                return recordType.getFieldNames();
            }
        }

        // Fallback: generate generic field names
        int numFields = records.get(0).size();
        String[] names = new String[numFields];
        for (int i = 0; i < numFields; i++) {
            names[i] = "field" + i;
        }
        return names;
    }

    /**
     * Infers the Avro type for a column by sampling record values.
     */
    private Schema.Type inferColumnType(List<Record> samples, int columnIndex) {
        for (Record sample : samples) {
            if (sample == null || columnIndex >= sample.size()) {
                continue;
            }
            Object value = sample.getField(columnIndex);
            if (value == null) {
                continue;
            }
            return this.toAvroType(value);
        }
        // Default to string if all values are null
        return Schema.Type.STRING;
    }

    /**
     * Maps a Java value to an Avro schema type.
     */
    private Schema.Type toAvroType(Object value) {
        if (value instanceof String || value instanceof Character) {
            return Schema.Type.STRING;
        } else if (value instanceof Integer) {
            return Schema.Type.INT;
        } else if (value instanceof Long || value instanceof Timestamp) {
            return Schema.Type.LONG;
        } else if (value instanceof Float) {
            return Schema.Type.FLOAT;
        } else if (value instanceof Double || value instanceof BigDecimal) {
            return Schema.Type.DOUBLE;
        } else if (value instanceof Boolean) {
            return Schema.Type.BOOLEAN;
        } else if (value instanceof byte[]) {
            return Schema.Type.BYTES;
        }
        return Schema.Type.STRING;
    }

    /**
     * Converts a Wayang Record to an Avro GenericRecord using the given schema.
     */
    private GenericRecord convertToGenericRecord(Record record, Schema schema) {
        GenericRecord genericRecord = new GenericData.Record(schema);
        List<Schema.Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            Object value = i < record.size() ? record.getField(i) : null;
            // Convert value to match the Avro type if needed
            if (value != null) {
                value = this.convertValue(value, fields.get(i).schema());
            }
            genericRecord.put(fields.get(i).name(), value);
        }
        return genericRecord;
    }

    /**
     * Converts a value to match the expected Avro schema type.
     */
    private Object convertValue(Object value, Schema fieldSchema) {
        // Handle nullable union types — extract the actual type
        Schema actualSchema = fieldSchema;
        if (fieldSchema.getType() == Schema.Type.UNION) {
            for (Schema s : fieldSchema.getTypes()) {
                if (s.getType() != Schema.Type.NULL) {
                    actualSchema = s;
                    break;
                }
            }
        }

        switch (actualSchema.getType()) {
            case STRING:
                return value.toString();
            case INT:
                return value instanceof Number ? ((Number) value).intValue() : Integer.parseInt(value.toString());
            case LONG:
                if (value instanceof Timestamp) return ((Timestamp) value).getTime();
                return value instanceof Number ? ((Number) value).longValue() : Long.parseLong(value.toString());
            case FLOAT:
                return value instanceof Number ? ((Number) value).floatValue() : Float.parseFloat(value.toString());
            case DOUBLE:
                return value instanceof Number ? ((Number) value).doubleValue() : Double.parseDouble(value.toString());
            case BOOLEAN:
                return value instanceof Boolean ? value : Boolean.parseBoolean(value.toString());
            default:
                return value;
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no outputs.");
    }

    @Override
    public JavaPlatform getPlatform() {
        return JavaPlatform.getInstance();
    }
}