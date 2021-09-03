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
