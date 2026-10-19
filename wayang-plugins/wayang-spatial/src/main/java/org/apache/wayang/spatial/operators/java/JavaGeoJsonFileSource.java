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

package org.apache.wayang.spatial.operators.java;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.spatial.data.WayangGeometry;
import org.apache.wayang.basic.operators.GeoJsonFileSource;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.operators.JavaExecutionOperator;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Stream;

/**
 * Java execution operator that parses a GeoJSON document and emits each feature as a {@link Record}.
 * Each emitted Record is created from the feature JSON text. The Record consists of the geometry and properties
 * of the feature (i.e., the Record's schema has two fields: "geometry" and "properties", where "geometry"
 * is of type {@link WayangGeometry} and "properties" is of type {@linkplain Map<String, Object>}).
 */
public class JavaGeoJsonFileSource extends GeoJsonFileSource implements JavaExecutionOperator {

    public JavaGeoJsonFileSource(String inputUrl) {
        super(inputUrl);
    }

    public JavaGeoJsonFileSource(GeoJsonFileSource that) {
        super(that);
    }

    public static Stream<Record> readFeatureCollectionFromFile(final String path) {
        try {
            final URI uri = URI.create(path);

            // use streaming parser to avoid loading entire file into memory
            ObjectMapper objectMapper = new ObjectMapper();
            JsonFactory jsonFactory = objectMapper.getFactory();
            List<Record> records = new ArrayList<>();

            try (InputStream in = Files.newInputStream(Paths.get(uri.getPath()));
                 JsonParser parser = jsonFactory.createParser(in)) {

                // advance to start object
                if (parser.nextToken() != JsonToken.START_OBJECT) {
                    throw new WayangException("Expected JSON object at root");
                }

                // find the "features" array
                while (parser.nextToken() != null) {
                    if (parser.currentToken() == JsonToken.FIELD_NAME
                            && "features".equals(parser.getCurrentName())) {
                        if (parser.nextToken() != JsonToken.START_ARRAY) {
                            throw new WayangException("Expected 'features' to be an array");
                        }
                        // iterate features
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            // parser is at START_OBJECT of a feature
                            JsonNode featureNode = objectMapper.readTree(parser);
                            JsonNode geometryNode = featureNode.path("geometry");
                            JsonNode propertiesNode = featureNode.path("properties");

                            String geometryJsonString = objectMapper.writeValueAsString(geometryNode);
                            WayangGeometry wayangGeometry = WayangGeometry.fromGeoJson(geometryJsonString);

                            Map<String, Object> propertiesMap = objectMapper.convertValue(propertiesNode, Map.class);

                            Record record = new Record();
                            record.addField(wayangGeometry);
                            record.addField(propertiesMap);
                            records.add(record);
                        }
                        break;
                    }
                }
            }
            return records.stream();
        } catch (final Exception e) {
            throw new WayangException(e);
        }
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final JavaExecutor javaExecutor,
            final OptimizationContext.OperatorContext operatorContext) {

        assert outputs.length == this.getNumOutputs();

        final String path = this.getInputUrl();
        final Stream<Record> wayangGeometryStream = readFeatureCollectionFromFile(path);

        ((StreamChannel.Instance) outputs[0]).accept(wayangGeometryStream);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public JavaGeoJsonFileSource copy() {
        return new JavaGeoJsonFileSource(this.getInputUrl());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }
}
