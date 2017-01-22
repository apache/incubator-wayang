package org.qcri.rheem.core.util;

import org.apache.commons.lang3.SerializationException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Utility to deal with {@link JsonSerializable}s.
 */
public class JsonSerializables {

    /**
     * Try to serialize the given {@link Object}. It must be JSON-compatible or a {@link JsonSerializable}.
     *
     * @param obj         the {@link Object} to serialize
     * @param isPolymorph in case a {@link JSONObject} is created, whether it should be tagged with the {@link Class}
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
     * @param isPolymorph  in case a {@link JSONObject} is created, whether it should be tagged with the {@link Class}
     *                     of {@code serializable}
     * @return the serialization result
     */
    public static JSONObject serialize(JsonSerializable serializable, boolean isPolymorph) {
        return serialize(serializable, isPolymorph, JsonSerializable.uncheckedSerializer);
    }

    /**
     * Serialize the given {@link Object} using a specific {@link JsonSerializer}.
     *
     * @param obj         the {@link Object} to serialize
     * @param isPolymorph in case a {@link JSONObject} is created, whether it should be tagged with the {@link Class}
     *                    of {@code serializable}
     * @param serializer  the {@link JsonSerializer}
     * @return the serialization result
     */
    public static <T> JSONObject serialize(T obj, boolean isPolymorph, JsonSerializer<T> serializer) {
        return addClassTag(obj, serializer.serialize(obj), isPolymorph);
    }

    /**
     * Try to serialize the given {@link Object}s. They must be JSON-compatible or a {@link JsonSerializable}s.
     *
     * @param collection  the {@link Object}s to serialize
     * @param isPolymorph in case a {@link JSONObject} is created, whether it should be tagged with the {@link Class}
     *                    of the according {@link Object}
     * @return the serialization result
     * @see #isJsonCompatible(Object)
     */
    public static JSONArray serializeAll(Collection<?> collection, boolean isPolymorph) {
        if (isJsonNull(collection)) return null;
        JSONArray jsonArray = new JSONArray();
        for (Object obj : collection) {
            jsonArray.put(serialize(obj, isPolymorph));
        }
        return jsonArray;
    }

    /**
     * Serialize the given {@link Object}s using a specific {@link JsonSerializer}.
     *
     * @param collection  the {@link Object}s to serialize
     * @param isPolymorph in case a {@link JSONObject} is created, whether it should be tagged with the {@link Class}
     *                    of {@code serializable}
     * @param serializer  the {@link JsonSerializer}
     * @return the serialization result
     */
    public static <T> JSONArray serializeAll(Collection<T> collection, boolean isPolymorph, JsonSerializer<T> serializer) {
        if (collection == null) return null;
        JSONArray jsonArray = new JSONArray();
        for (T serializable : collection) {
            jsonArray.put(serialize(serializable, isPolymorph, serializer));
        }
        return jsonArray;
    }

    /**
     * Deserialize a given JSON datatype. The following cases are supported:
     * <ul>
     * <li>{@code json} is a (JSON) {@code null} value;</li>
     * <li>{@code json} is a basic (JSON) datatype;</li>
     * <li>{@code json} is a {@link Class}-tagged {@link JSONObject} that corresponds to a {@link JsonSerializable};</li>
     * <li>{@code json} is a {@link JSONArray} with {@link Class}-tagged {@link JSONObject}s that correspond to a
     * {@link JsonSerializable}s - in this case, the result type is a {@link List}.</li>
     * </ul>
     *
     * @param json the JSON data
     * @return the deserialization result
     */
    public static Object deserialize(Object json) {
        if (isJsonNull(json)) return null;
        else if (isUnconvertedInstance(json)) return json;
        else if (json instanceof JSONObject) return deserialize((JSONObject) json);
        else if (json instanceof JSONArray) return deserializeAllAsList((JSONArray) json);

        throw new SerializationException(String.format("Don't know how to deserialize %s.", json));
    }

    /**
     * Deserialize a {@link Class}-tagged {@link JSONObject} that should correspond to a {@link JsonSerializable}.
     *
     * @param jsonObject the {@link JSONObject}
     * @return the deserialization product
     */
    @SuppressWarnings("unchecked")
    public static <T> T deserialize(JSONObject jsonObject) {
        if (isJsonNull(jsonObject)) return null;
        return deserialize(jsonObject, JsonSerializable.uncheckedSerializer());
    }

    /**
     * Deserialize a {@link JSONObject} that should correspond to a {@link JsonSerializable}.
     *
     * @param jsonObject the {@link JSONObject}
     * @param cls        the {@link Class} of the deserialization product
     * @return the deserialization product
     */
    @SuppressWarnings("unchecked")
    public static <T> T deserialize(JSONObject jsonObject, Class<? extends T> cls) {
        if (isJsonNull(jsonObject)) return null;
        return deserialize(jsonObject, (JsonSerializer<T>) JsonSerializable.uncheckedSerializer(), cls);
    }

    /**
     * Deserialize a ({@link Class}-tagged) {@link JSONObject} with a {@link JsonSerializer}.
     *
     * @param jsonObject the {@link JSONObject}
     * @param serializer the {@link JsonSerializer}
     * @return the deserialization product
     */
    public static <T> T deserialize(JSONObject jsonObject, JsonSerializer<T> serializer) {
        if (isJsonNull(jsonObject)) return null;
        return serializer.deserialize(jsonObject);
    }

    /**
     * Deserialize a {@link JSONObject} with a {@link JsonSerializer}.
     *
     * @param jsonObject the {@link JSONObject}
     * @param serializer the {@link JsonSerializer}
     * @param cls        the {@link Class} of the deserialization product
     * @return the deserialization product
     */
    public static <T> T deserialize(JSONObject jsonObject, JsonSerializer<T> serializer, Class<? extends T> cls) {
        if (jsonObject == null || jsonObject.equals(JSONObject.NULL)) return null;
        return serializer.deserialize(jsonObject, cls);
    }

    /**
     * Deserialize a {@link JSONArray} according to the rules of {@link #deserialize(Object)}.
     *
     * @param jsonArray the {@link JSONArray}
     * @return the deserialization product
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> deserializeAllAsList(JSONArray jsonArray) {
        if (isJsonNull(jsonArray)) return null;
        List<T> result = new ArrayList<>(jsonArray.length());
        for (Object jsonElement : jsonArray) {
            result.add((T) deserialize(jsonElement));
        }
        return result;
    }

    /**
     * Deserialize a {@link JSONArray} according to the rules of {@link #deserialize(JSONObject, Class)}.
     *
     * @param jsonArray the {@link JSONArray}
     * @param cls       the {@link Class} of the elements in the {@code jsonArray}
     * @return the deserialization product
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> deserializeAllAsList(JSONArray jsonArray, Class<T> cls) {
        return deserializeAllAsList(jsonArray, (JsonSerializer<T>) JsonSerializable.uncheckedSerializer(), cls);
    }

    /**
     * Deserialize a {@link JSONArray} according to the rules of {@link #deserialize(JSONObject, JsonSerializer)}.
     *
     * @param jsonArray  the {@link JSONArray}
     * @param serializer the {@link JsonSerializer}
     * @return the deserialization product
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> deserializeAllAsList(JSONArray jsonArray, JsonSerializer<T> serializer) {
        List<T> result = new ArrayList<>(jsonArray.length());
        for (Object jsonElement : jsonArray) {
            result.add(isJsonNull(jsonElement) ? null : deserialize((JSONObject) jsonElement, serializer));
        }
        return result;
    }

    /**
     * Deserialize a {@link JSONArray} according to the rules of {@link #deserialize(JSONObject, JsonSerializer, Class)}.
     *
     * @param jsonArray  the {@link JSONArray}
     * @param serializer the {@link JsonSerializer}
     * @param cls        the {@link Class} of the elements in the {@code jsonArray}
     * @return the deserialization product
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> deserializeAllAsList(JSONArray jsonArray, JsonSerializer<T> serializer, Class<T> cls) {
        List<T> result = new ArrayList<>(jsonArray.length());
        for (Object jsonElement : jsonArray) {
            result.add(isJsonNull(jsonElement) ? null : deserialize((JSONObject) jsonElement, serializer, cls));
        }
        return result;
    }

    /**
     * Tag the {@link JSONObject} with the {@link Class} of the other {@link Object}.
     *
     * @param obj        whose {@link Class} should be tagged
     * @param jsonObject that should be tagged
     * @return the {@code jsonObject}
     */

    public static JSONObject addClassTag(Object obj, JSONObject jsonObject) {
        if (isJsonNull(jsonObject)) return jsonObject;
        return jsonObject.put("_class", obj.getClass().getCanonicalName());
    }

    /**
     * Optionally tag the {@link JSONObject} with the {@link Class} of the other {@link Object}.
     *
     * @param obj           whose {@link Class} should be tagged
     * @param jsonObject    that should be tagged
     * @param isAddClassTag if this is {@code false}, no action will be performed
     * @return the {@code jsonObject}
     */
    public static JSONObject addClassTag(Object obj, JSONObject jsonObject, boolean isAddClassTag) {
        return isAddClassTag ? addClassTag(obj, jsonObject) : jsonObject;
    }

    /**
     * Read and load the specified {@link Class} in a {@link JSONObject}.
     *
     * @param jsonObject that contains the {@link Class} tag
     * @return the loaded {@link Class} or {@code null} if none exists
     * @throws ClassNotFoundException if the {@link Class} could not be loaded
     */
    public static Class<?> getClassTag(JSONObject jsonObject) throws ClassNotFoundException {
        if (isJsonNull(jsonObject) || !jsonObject.has("_class")) return null;
        final String className = jsonObject.getString("_class");
        return Class.forName(className);
    }

    /**
     * Tells whether the given instance is a (JSON) {@code null} value.
     *
     * @param obj the instance to test
     * @return whether {@code obj} is a (JSON) {@code null} value
     */
    public static boolean isJsonNull(Object obj) {
        return obj == null || JSONObject.NULL.equals(obj);
    }

    /**
     * Tells whether the given instance is a JSON datatype.
     *
     * @param obj the instance to test
     * @return whether {@code obj} is a JSON datatype
     */
    public static boolean isJsonCompatible(Object obj) {
        return isUnconvertedInstance(obj) || obj == JSONObject.NULL || obj instanceof JSONObject || obj instanceof JSONArray;
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
