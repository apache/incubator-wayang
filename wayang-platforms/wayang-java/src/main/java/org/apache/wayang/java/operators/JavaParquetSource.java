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

import org.apache.avro.SchemaBuilder;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.ParquetSource;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This is execution operator implements the {@link ParquetSource}.
 */
public class JavaParquetSource extends ParquetSource implements JavaExecutionOperator {

    public JavaParquetSource(String inputUrl, String[] projection) {
        super(ParquetSource.create(inputUrl, projection));
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaParquetSource(ParquetSource that) { super(that); }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        String urlStr = this.getInputUrl();

        try {
            Configuration conf = new Configuration();

            // Define a projection schema, if any (uses default schema if no projection defined)
            Schema schema = getSchemaToRead();
            AvroReadSupport.setAvroReadSchema(conf, schema);
            AvroReadSupport.setRequestedProjection(conf, schema);

            InputFile file = HadoopInputFile.fromPath(new Path(urlStr), conf);

            // Parse dates as logical types
            GenericData model = new GenericData();
            model.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
            model.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
            model.addLogicalTypeConversion(new TimeConversions.DateConversion());
            model.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
            model.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());

            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file)
                    .withDataModel(model)
                    .build();

            List<Record> records = new ArrayList<>();
            GenericRecord record;

            while ((record = reader.read()) != null) {
                records.add(convertGenericRecordToRecord(record));
            }

            ((StreamChannel.Instance) outputs[0]).accept(records);

        } catch (Exception e) {
            throw new WayangException(String.format("Reading from Parquet file %s failed.", urlStr), e);
        }

        ExecutionLineageNode prepareLineageNode = new ExecutionLineageNode(operatorContext);
        prepareLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.parquetsource.load.prepare", javaExecutor.getConfiguration()
        ));
        ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);
        mainLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.parquetsource.load.main", javaExecutor.getConfiguration()
        ));

        outputs[0].getLineage().addPredecessor(mainLineageNode);

        return prepareLineageNode.collectAndMark();
    }

    private Schema getSchemaToRead() {
        String[] projection = this.getProjection();
        Schema avroSchema = new AvroSchemaConverter().convert(this.getSchema());

        if (projection == null || projection.length == 0) {
            return avroSchema;
        }

        Set<String> projectionSet = Set.of(projection);

        List<Schema.Field> filteredFields = avroSchema.getFields().stream()
                .filter(field -> projectionSet.contains(field.name()))
                .collect(Collectors.toList());

        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(avroSchema.getName())
                .namespace(avroSchema.getNamespace())
                .fields();

        for (Field field : filteredFields) {
            fieldAssembler = fieldAssembler.name(field.name()).type(field.schema()).noDefault();
        }

        return fieldAssembler.endRecord();
    }

    private Record convertGenericRecordToRecord(GenericRecord record) {
        List<Object> values = record.getSchema().getFields().stream()
                .map(field -> record.get(field.name()))
                .collect(Collectors.toList());
        return new Record(values);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("wayang.java.parquetsource.load.prepare", "wayang.java.parquetsource.load.main");
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
