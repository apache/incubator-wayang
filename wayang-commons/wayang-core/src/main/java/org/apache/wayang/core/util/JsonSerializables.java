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


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.wayang.core.util.json.WayangJsonArray;
import org.apache.wayang.core.util.json.WayangJsonObj;

/**
 * Utility to deal with {@link JsonSerializable}s.
 */
public class JsonSerializables {

    /**
     * Try to serialize the given {@link Object}. It must be JSON-compatible or a {@link JsonSerializable}.
     *
     * @param obj         the {@link Object} to serialize
     * @param isPolymorph in case a {@link WayangJsonObj} is created, whether it should be tagged with the {@link Class}
     *                    of {@code obj}
     * @return the serialization result
     * @see #isJsonCompatible(Object)
     */
    public static Object serialize(Object obj, boolean isPolymorph) {
        if (obj == null) return null;
        if (isJsonCompatible(obj)) {
            return obj;
        }
        if (obj instanceof JsonSerializable) return serialize((JsonSerializable) obj, isPolymorph);
        throw new IllegalArgumentException(String.format("Cannot serialize %s.", obj));
    }

    /**
     * Serialize the given {@link JsonSerializable}.
     *
     * @param serializable the {@link JsonSerializable} to serialize
     * @param isPolymorph  in case a {@link WayangJsonObj} is created, whether it should be tagged with the {@link Class}
     *                     of {@code serializable}
     * @return the serialization result
     */
    public static WayangJsonObj serialize(JsonSerializable serializable, boolean isPolymorph) {
        return serialize(serializable, isPolymorph, JsonSerializable.uncheckedSerializer);
    }

    /**
     * Serialize the given {@link Object} using a specific {@link JsonSerializer}.
     *
     * @param obj         the {@link Object} to serialize
     * @param isPolymorph in case a {@link WayangJsonObj} is created, whether it should be tagged with the {@link Class}
     *                    of {@code serializable}
     * @param serializer  the {@link JsonSerializer}
     * @return the serialization result
     */
    public static <T> WayangJsonObj serialize(T obj, boolean isPolymorph, JsonSerializer<T> serializer) {
        return addClassTag(obj, serializer.serialize(obj), isPolymorph);
    }

    /**
     * Try to serialize the given {@link Object}s. They must be JSON-compatible or a {@link JsonSerializable}s.
     *
     * @param collection  the {@link Object}s to serialize
     * @param isPolymorph in case a {@link WayangJsonObj} is created, whether it should be tagged with the {@link Class}
     *                    of the according {@link Object}
     * @return the serialization result
     * @see #isJsonCompatible(Object)
     */
    public static WayangJsonArray serializeAll(Collection<?> collection, boolean isPolymorph) {
        if (isJsonNull(collection)) return null;
        WayangJsonArray wayangJsonArray = new WayangJsonArray();
        for (Object obj : collection) {
            wayangJsonArray.put(serialize(obj, isPolymorph));
        }
        return wayangJsonArray;
    }

    /**
     * Serialize the given {@link Object}s using a specific {@link JsonSerializer}.
     *
     * @param collection  the {@link Object}s to serialize
     * @param isPolymorph in case a {@link WayangJsonObj} is created, whether it should be tagged with the {@link Class}
     *                    of {@code serializable}
     * @param serializer  the {@link JsonSerializer}
     * @return the serialization result
     */
    public static <T> WayangJsonArray serializeAll(Collection<T> collection, boolean isPolymorph, JsonSerializer<T> serializer) {
        if (collection == null) return null;
        WayangJsonArray wayangJsonArray = new WayangJsonArray();
        for (T serializable : collection) {
            wayangJsonArray.put(serialize(serializable, isPolymorph, serializer));
        }
        return wayangJsonArray;
    }

    /**
     * Deserialize a given JSON datatype. The following cases are supported:
     * <ul>
     * <li>{@code json} is a (JSON) {@code null} value;</li>
     * <li>{@code json} is a basic (JSON) datatype;</li>
     * <li>{@code json} is a {@link Class}-tagged {@link WayangJsonObj} that corresponds to a {@link JsonSerializable};</li>
     * <li>{@code json} is a {@link WayangJsonArray} with {@link Class}-tagged {@link WayangJsonObj}s that correspond to a
     * {@link JsonSerializable}s - in this case, the result type is a {@link List}.</li>
     * </ul>
     *
     * @param json the JSON data
     * @return the deserialization result
     */
    public static Object deserialize(Object json) {
        if (isJsonNull(json)) return null;
        else if (isUnconvertedInstance(json)) return json;
        else if (json instanceof WayangJsonObj) return deserialize((WayangJsonObj) json);
        else if (json instanceof WayangJsonArray) return deserializeAllAsList((WayangJsonArray) json);

        throw new SerializationException(String.format("Don't know how to deserialize %s.", json));
    }

    /**
     * Deserialize a {@link Class}-tagged {@link WayangJsonObj} that should correspond to a {@link JsonSerializable}.
     *
     * @param wayangJsonObj the {@link WayangJsonObj}
     * @return the deserialization product
     */
    @SuppressWarnings("unchecked")
    public static <T> T deserialize(WayangJsonObj wayangJsonObj) {
        if (isJsonNull(wayangJsonObj)) return null;
        return deserialize(wayangJsonObj, JsonSerializable.uncheckedSerializer());
    }

    /**
     * Deserialize a {@link WayangJsonObj} that should correspond to a {@link JsonSerializable}.
     *
     * @param wayangJsonObj the {@link WayangJsonObj}
     * @param cls        the {@link Class} of the deserialization product
     * @return the deserialization product
     */
    @SuppressWarnings("unchecked")
    public static <T> T deserialize(WayangJsonObj wayangJsonObj, Class<? extends T> cls) {
        if (isJsonNull(wayangJsonObj)) return null;
        return deserialize(wayangJsonObj, (JsonSerializer<T>) JsonSerializable.uncheckedSerializer(), cls);
    }

    /**
     * Deserialize a ({@link Class}-tagged) {@link WayangJsonObj} with a {@link JsonSerializer}.
     *
     * @param wayangJsonObj the {@link WayangJsonObj}
     * @param serializer the {@link JsonSerializer}
     * @return the deserialization product
     */
    public static <T> T deserialize(WayangJsonObj wayangJsonObj, JsonSerializer<T> serializer) {
        if (isJsonNull(wayangJsonObj)) return null;
        return serializer.deserialize(wayangJsonObj);
    }

    /**
     * Deserialize a {@link WayangJsonObj} with a {@link JsonSerializer}.
     *
     * @param wayangJsonObj the {@link WayangJsonObj}
     * @param serializer the {@link JsonSerializer}
     * @param cls        the {@link Class} of the deserialization product
     * @return the deserialization product
     */
    public static <T> T deserialize(WayangJsonObj wayangJsonObj, JsonSerializer<T> serializer, Class<? extends T> cls) {
        if (wayangJsonObj == null || wayangJsonObj.equals(WayangJsonObj.NULL)) return null;
        return serializer.deserialize(wayangJsonObj, cls);
    }

    /**
     * Deserialize a {@link WayangJsonArray} according to the rules of {@link #deserialize(Object)}.
     *
     * @param wayangJsonArray the {@link WayangJsonArray}
     * @return the deserialization product
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> deserializeAllAsList(WayangJsonArray wayangJsonArray) {
        if (isJsonNull(wayangJsonArray)) return null;
        List<T> result = new ArrayList<>(wayangJsonArray.length());
        for (Object jsonElement : wayangJsonArray) {
            result.add((T) deserialize(jsonElement));
        }
        return result;
    }

    /**
     * Deserialize a {@link WayangJsonArray} according to the rules of {@link #deserialize(WayangJsonObj, Class)}.
     *
     * @param wayangJsonArray the {@link WayangJsonArray}
     * @param cls       the {@link Class} of the elements in the {@code jsonArray}
     * @return the deserialization product
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> deserializeAllAsList(WayangJsonArray wayangJsonArray, Class<T> cls) {
        return deserializeAllAsList(wayangJsonArray, (JsonSerializer<T>) JsonSerializable.uncheckedSerializer(), cls);
    }

    /**
     * Deserialize a {@link WayangJsonArray} according to the rules of {@link #deserialize(WayangJsonObj, JsonSerializer)}.
     *
     * @param wayangJsonArray  the {@link WayangJsonArray}
     * @param serializer the {@link JsonSerializer}
     * @return the deserialization product
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> deserializeAllAsList(WayangJsonArray wayangJsonArray, JsonSerializer<T> serializer) {
        List<T> result = new ArrayList<>(wayangJsonArray.length());
        for (Object jsonElement : wayangJsonArray) {
            result.add(isJsonNull(jsonElement) ? null : deserialize((WayangJsonObj) jsonElement, serializer));
        }
        return result;
    }

    /**
     * Deserialize a {@link WayangJsonArray} according to the rules of {@link #deserialize(WayangJsonObj, JsonSerializer, Class)}.
     *
     * @param wayangJsonArray  the {@link WayangJsonArray}
     * @param serializer the {@link JsonSerializer}
     * @param cls        the {@link Class} of the elements in the {@code jsonArray}
     * @return the deserialization product
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> deserializeAllAsList(WayangJsonArray wayangJsonArray, JsonSerializer<T> serializer, Class<T> cls) {
        List<T> result = new ArrayList<>(wayangJsonArray.length());
        for (Object jsonElement : wayangJsonArray) {
            result.add(isJsonNull(jsonElement) ? null : deserialize((WayangJsonObj) jsonElement, serializer, cls));
        }
        return result;
    }

    /**
     * Tag the {@link WayangJsonObj} with the {@link Class} of the other {@link Object}.
     *
     * @param obj        whose {@link Class} should be tagged
     * @param wayangJsonObj that should be tagged
     * @return the {@code jsonObject}
     */

    public static WayangJsonObj addClassTag(Object obj, WayangJsonObj wayangJsonObj) {
        if (isJsonNull(wayangJsonObj)) return wayangJsonObj;
        return wayangJsonObj.put("_class", obj.getClass().getCanonicalName());
    }

    /**
     * Optionally tag the {@link WayangJsonObj} with the {@link Class} of the other {@link Object}.
     *
     * @param obj           whose {@link Class} should be tagged
     * @param wayangJsonObj    that should be tagged
     * @param isAddClassTag if this is {@code false}, no action will be performed
     * @return the {@code jsonObject}
     */
    public static WayangJsonObj addClassTag(Object obj, WayangJsonObj wayangJsonObj, boolean isAddClassTag) {
        return isAddClassTag ? addClassTag(obj, wayangJsonObj) : wayangJsonObj;
    }

    /**
     * Read and load the specified {@link Class} in a {@link WayangJsonObj}.
     *
     * @param wayangJsonObj that contains the {@link Class} tag
     * @return the loaded {@link Class} or {@code null} if none exists
     * @throws ClassNotFoundException if the {@link Class} could not be loaded
     */
    public static Class<?> getClassTag(WayangJsonObj wayangJsonObj) throws ClassNotFoundException {
        if (isJsonNull(wayangJsonObj) || !wayangJsonObj.has("_class")) return null;
        final String className = wayangJsonObj.getString("_class");
        return Class.forName(className);
    }

    /**
     * Tells whether the given instance is a (JSON) {@code null} value.
     *
     * @param obj the instance to test
     * @return whether {@code obj} is a (JSON) {@code null} value
     */
    public static boolean isJsonNull(Object obj) {
        return obj == null || WayangJsonObj.NULL.equals(obj);
    }

    /**
     * Tells whether the given instance is a JSON datatype.
     *
     * @param obj the instance to test
     * @return whether {@code obj} is a JSON datatype
     */
    public static boolean isJsonCompatible(Object obj) {
        return isUnconvertedInstance(obj) || obj == WayangJsonObj.NULL || obj instanceof WayangJsonObj
            || obj instanceof WayangJsonArray;
    }

    /**
     * Tells whether the given instance does not require conversion, which is the case for {@link Long}s, {@link Integer}s,
     * {@link Double}s, {@link String}s, and {@code null}s.
     *
     * @param obj the instance to test
     * @return whether {@code obj} does not require conversion
     */
    public static boolean isUnconvertedInstance(Object obj) {
        return obj == null || obj instanceof Long || obj instanceof Integer || obj instanceof Double || obj instanceof String;
    }

}
