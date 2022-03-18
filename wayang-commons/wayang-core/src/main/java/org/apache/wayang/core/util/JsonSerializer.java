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

import org.apache.commons.lang3.SerializationException;
import org.apache.wayang.core.util.json.WayangJsonObj;


/**
 * Alternative to {@link JsonSerializable}: Externalizes serialization logic.
 */
public interface JsonSerializer<T> {

    /**
     * Serializes an object.
     *
     * @param object that should be serialized
     * @return the serialized object
     */
    WayangJsonObj serialize(T object);

    /**
     * Deserializes an object.
     *
     * @param json that should be serialized
     * @return the deserialized object
     */
    @SuppressWarnings("unchecked")
    default T deserialize(WayangJsonObj json) {
        if (JsonSerializables.isJsonNull(json)) return null;
        try {
            final Class<?> classTag = JsonSerializables.getClassTag(json);
            if (classTag == null) {
                throw new IllegalArgumentException(String.format("Cannot determine class from %s.", json));
            }
            return this.deserialize(json, (Class<? extends T>) classTag);
        } catch (ClassNotFoundException e) {
            throw new SerializationException("Could not load class.", e);
        }
    }

    /**
     * Deserializes an object.
     *
     * @param json that should be serialized
     * @param cls  the {@link Class} of the object to be created
     * @return the deserialized object
     */

    T deserialize(WayangJsonObj json, Class<? extends T> cls);

}
