/*
 * Copyright 2016 Sebastian Kruse,
 * code from: https://github.com/sekruse/profiledb-java.git
 * Original license:
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.wayang.commons.util.profiledb.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.wayang.commons.util.profiledb.model.Measurement;

import java.lang.reflect.Type;

/**
 * Custom serializer for {@link Measurement}s
 * Detects actual subclass of given instances, encodes this class membership, and then delegates serialization to that subtype.
 */
public class MeasurementSerializer implements JsonSerializer<Measurement> {

    @Override
    public JsonElement serialize(Measurement measurement, Type type, JsonSerializationContext jsonSerializationContext) {
        final JsonObject jsonObject = (JsonObject) jsonSerializationContext.serialize(measurement);
        jsonObject.addProperty("type", measurement.getType());
        return jsonObject;
    }
}
