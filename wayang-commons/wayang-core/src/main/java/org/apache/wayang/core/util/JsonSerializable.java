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

package org.apache.wayang.core.util;

import org.apache.wayang.core.api.exception.WayangException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.wayang.core.util.json.WayangJsonObj;

/**
 * This interface prescribes implementing instances to be able to provide itself as a {@link WayangJsonObj}. To allow
 * for deserialization, implementing class should furthermore provide a static {@code fromJson(JSONObject)} method.
 * <i>Note that it is recommended to use the {@link JsonSerializables} utility to class to handle serialization.</i>
 *
 * @see JsonSerializables
 */
public interface JsonSerializable {

    /**
     * Convert this instance to a {@link WayangJsonObj}.
     *
     * @return the {@link WayangJsonObj}
     */
    WayangJsonObj toJson();

    /**
     * A {@link JsonSerializer} implementation to serialize {@link JsonSerializable}s.
     */
    Serializer<JsonSerializable> uncheckedSerializer = new Serializer<>();

    /**
     * A {@link JsonSerializer} implementation to serialize {@link JsonSerializable}s.
     */
    @SuppressWarnings("unchecked")
    static <T extends JsonSerializable> Serializer<T> uncheckedSerializer() {
        return (Serializer<T>) uncheckedSerializer;
    }

    /**
     * A {@link JsonSerializer} implementation to serialize {@link JsonSerializable}s.
     */
    class Serializer<T extends JsonSerializable> implements JsonSerializer<T> {

        @Override
        public WayangJsonObj serialize(T serializable) {
            if (serializable == null) return null;
            return serializable.toJson();
        }

        @Override
        @SuppressWarnings("unchecked")
        public T deserialize(WayangJsonObj json, Class<? extends T> cls) {
            if (json == null || json.equals(WayangJsonObj.NULL)) return null;
            try {
                final Method fromJsonMethod = cls.getMethod("fromJson", WayangJsonObj.class);
                return (T) fromJsonMethod.invoke(null, json);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new WayangException(String.format("Could not execute %s.fromJson(...).", cls.getCanonicalName()), e);
            }
        }
    }

}
