package org.qcri.rheem.core.util;

import org.json.JSONArray;
import org.json.JSONObject;
import org.qcri.rheem.core.api.exception.RheemException;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Utility to deal with {@link JsonSerializable}s.
 */
public class JsonSerializables {

    public static Object serialize(Object object) {
        return serialize(object, false);
    }

    public static JSONObject serializeAtLeastClass(Object object) {
        if (object == null) return null;
        if (object instanceof JsonSerializable) return (JSONObject) serialize(object, true);
        return new JSONObject().put("_class", object.getClass().getCanonicalName());
    }

    public static Object serialize(Object obj, boolean isPolymorph) {
        if (obj == null) return null;
        if (obj instanceof Long || obj instanceof Integer || obj instanceof Double || obj instanceof String
                || obj == JSONObject.NULL
                || obj instanceof JSONObject || obj instanceof JSONArray) {
            return obj;
        }
        if (obj instanceof JsonSerializable) return serialize((JsonSerializable) obj, isPolymorph);
        throw new IllegalArgumentException(String.format("Cannot serialize %s.", obj));
    }

    public static JSONObject serialize(JsonSerializable serializable) {
        return serialize(serializable, false);
    }

    public static JSONObject serialize(JsonSerializable serializable, boolean isPolymorph) {
        if (serializable == null) return null;
        JSONObject jsonObject = serializable.toJson();
        if (isPolymorph) jsonObject.put("_class", serializable.getClass().getCanonicalName());
        return jsonObject;
    }

    public static JSONArray serializeAll(JsonSerializable[] serializables) {
        return serializeAll(serializables, false);
    }

    public static JSONArray serializeAll(JsonSerializable[] serializables, boolean isPolymorph) {
        if (serializables == null) return null;
        JSONArray jsonArray = new JSONArray();
        for (int i = 0; i < serializables.length; i++) {
            JsonSerializable serializable = serializables[i];
            jsonArray.put(serialize(serializable, isPolymorph));
        }
        return jsonArray;
    }

    public static JSONArray serializeAll(Collection<?> serializables) {
        return serializeAll(serializables, false);
    }

    public static JSONArray serializeAll(Collection<?> serializables, boolean isPolymorph) {
        if (serializables == null) return null;
        JSONArray jsonArray = new JSONArray();
        for (Object serializable : serializables) {
            jsonArray.put(serialize(serializable, isPolymorph));
        }
        return jsonArray;
    }

    @SuppressWarnings("unchecked")
    public static <T> T deserializeAtLeastClass(JSONObject jsonObject, List<Tuple<Class<?>, Supplier<?>>> argSuppliers) {
        if (jsonObject == null || jsonObject.equals(JSONObject.NULL)) return null;
        try {
            return deserialize(jsonObject);
        } catch (Exception e) {
        }
        final String className = jsonObject.getString("_class");
        final Class<?> cls;
        try {
            cls = Class.forName(className);
            return (T) ReflectionUtils.instantiateSomehow(cls, argSuppliers);
        } catch (ClassNotFoundException e1) {
            throw new RheemException(String.format("Could not deserialize %s.", jsonObject), e1);
        }
    }

    public static Object deserialize(Object o) {
        if (o == null || o.equals(JSONObject.NULL)) return null;
        if (o instanceof JSONObject) {
            return deserialize((JSONObject) o);
        } else if (o instanceof JSONArray) {
            deserializeAllAsList((JSONArray) o);
        }
        return o;
    }

    @SuppressWarnings("unchecked")
    public static <T> T deserialize(JSONObject jsonObject) {
        if (jsonObject == null || jsonObject.equals(JSONObject.NULL)) return null;
        final String className = jsonObject.getString("_class");
        final Class cls;
        try {
            cls = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RheemException(String.format("Could not deserialize %s.", jsonObject), e);
        }
        return (T) deserialize(jsonObject, cls);
    }

    @SuppressWarnings("unchecked")
    public static <T> T deserialize(Object jsonObject, Class<T> cls) {
        if (jsonObject == null || jsonObject.equals(JSONObject.NULL)) return null;
        try {
            final Method fromJsonMethod = cls.getMethod("fromJson", JSONObject.class);
            return (T) fromJsonMethod.invoke(null, jsonObject);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RheemException(String.format("Could not execute %s.fromJson(...).", cls.getCanonicalName()), e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> deserializeAllAsList(JSONArray jsonArray) {
        List<T> result = new ArrayList<>(jsonArray.length());
        for (Object jsonElement : jsonArray) {
            result.add((T) deserialize(jsonElement));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> deserializeAllAsList(JSONArray jsonArray, Class<T> cls) {
        List<T> result = new ArrayList<>(jsonArray.length());
        for (Object jsonElement : jsonArray) {
            result.add(deserialize(jsonElement, cls));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public static Object[] deserializeAllAsArray(JSONArray jsonArray) {
        Object[] result = new Object[jsonArray.length()];
        int i = 0;
        for (Object jsonElement : jsonArray) {
            result[i++] = deserialize(jsonElement);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public static <T> T[] deserializeAllAsArray(JSONArray jsonArray, Class<T> cls) {
        T[] result = (T[]) Array.newInstance(cls, jsonArray.length());
        int i = 0;
        for (Object jsonElement : jsonArray) {
            result[i++] = deserialize(jsonElement, cls);
        }
        return result;
    }
}
