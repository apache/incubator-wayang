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

package org.apache.wayang.commons.util.profiledb.json;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import org.apache.wayang.commons.util.profiledb.model.Measurement;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom deserializer for {@link Measurement}s
 * Detects actual subclass of serialized instances and then delegates the deserialization to that subtype.
 */
public class MeasurementDeserializer implements JsonDeserializer<Measurement> {

    private final Map<String, Class<? extends Measurement>> measurementTypes = new HashMap<>();

    public void register(Class<? extends Measurement> measurementClass) {
        String typeName = Measurement.getTypeName(measurementClass);
        this.measurementTypes.put(typeName, measurementClass);
    }

    @Override
    public Measurement deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        final JsonElement typeElement = jsonElement.getAsJsonObject().get("type");
        if (typeElement == null) {
            throw new IllegalArgumentException("Missing type in " + jsonElement);
        }
        final String typeName = typeElement.getAsString();
        final Class<? extends Measurement> measurementClass = this.measurementTypes.get(typeName);
        if (measurementClass == null) {
            throw new JsonParseException("Unknown measurement type: " + typeName);
        }
        return jsonDeserializationContext.deserialize(jsonElement, measurementClass);
    }

}
