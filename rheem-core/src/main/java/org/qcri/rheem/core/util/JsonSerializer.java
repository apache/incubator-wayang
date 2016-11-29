package org.qcri.rheem.core.util;

import org.apache.commons.lang3.SerializationException;
import org.json.JSONObject;

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
    JSONObject serialize(T object);

    /**
     * Deserializes an object.
     *
     * @param json that should be serialized
     * @return the deserialized object
     */
    @SuppressWarnings("unchecked")
    default T deserialize(JSONObject json) {
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

    T deserialize(JSONObject json, Class<? extends T> cls);

}
