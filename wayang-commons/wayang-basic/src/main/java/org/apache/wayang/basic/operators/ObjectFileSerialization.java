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

package org.apache.wayang.basic.operators;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import org.apache.commons.lang3.Validate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Utility methods that convert between Java objects and the on-disk representation used by {@link ObjectFileSink}.
 */
public final class ObjectFileSerialization {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    static {
        OBJECT_MAPPER.findAndRegisterModules();
    }

    private ObjectFileSerialization() {
    }

    /**
     * Serialize a chunk of objects using the provided {@link ObjectFileSerializationMode}.
     *
     * @param chunk       buffer that contains the objects to serialize
     * @param validLength number of valid entries inside {@code chunk}
     * @param mode        the serialization mode
     * @return serialized bytes
     * @throws IOException if serialization fails
     */
    public static byte[] serializeChunk(Object[] chunk, int validLength, ObjectFileSerializationMode mode) throws IOException {
        Validate.notNull(mode, "Serialization mode must be provided.");
        switch (mode) {
            case JSON:
                return serializeJson(chunk, validLength);
            case LEGACY_JAVA_SERIALIZATION:
                return serializeLegacy(chunk, validLength);
            default:
                throw new IllegalArgumentException("Unknown serialization mode: " + mode);
        }
    }

    /**
     * Deserialize a chunk of objects.
     *
     * @param payload     the serialized data
     * @param mode        the serialization mode
     * @param elementType the expected element type
     * @return list of deserialized objects (never {@code null})
     * @throws IOException            if deserialization fails
     * @throws ClassNotFoundException if a class cannot be resolved in legacy mode
     */
    public static List<Object> deserializeChunk(byte[] payload,
                                                ObjectFileSerializationMode mode,
                                                Class<?> elementType) throws IOException, ClassNotFoundException {
        Validate.notNull(mode, "Serialization mode must be provided.");
        switch (mode) {
            case JSON:
                return deserializeJson(payload, elementType);
            case LEGACY_JAVA_SERIALIZATION:
                return deserializeLegacy(payload);
            default:
                throw new IllegalArgumentException("Unknown serialization mode: " + mode);
        }
    }

    private static byte[] serializeLegacy(Object[] chunk, int validLength) throws IOException {
        Object[] payload = Arrays.copyOf(chunk, validLength);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(payload);
            oos.flush();
            return bos.toByteArray();
        }
    }

    private static List<Object> deserializeLegacy(byte[] payload) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(payload);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            Object tmp = ois.readObject();
            if (tmp == null) {
                return Collections.emptyList();
            }
            if (tmp instanceof Collection) {
                return new ArrayList<>((Collection<?>) tmp);
            }
            if (tmp.getClass().isArray()) {
                return Arrays.asList((Object[]) tmp);
            }
            return new ArrayList<>(Collections.singletonList(tmp));
        }
    }

    private static byte[] serializeJson(Object[] chunk, int validLength) throws IOException {
        Object[] payload = Arrays.copyOf(chunk, validLength);
        return OBJECT_MAPPER.writeValueAsBytes(payload);
    }

    private static List<Object> deserializeJson(byte[] payload, Class<?> elementType) throws IOException {
        Validate.notNull(elementType, "Element type must be provided for JSON deserialization.");
        CollectionType type = OBJECT_MAPPER.getTypeFactory()
                .constructCollectionType(List.class, elementType);
        List<?> list = OBJECT_MAPPER.readValue(payload, type);
        if (list == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(list);
    }
}
