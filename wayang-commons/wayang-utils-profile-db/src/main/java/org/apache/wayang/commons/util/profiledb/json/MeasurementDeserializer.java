/*
 * Copyright 2016 Sebastian Kruse,
 * code from: https://github.com/sekruse/profiledb-java.git
 * Original license:
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
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
