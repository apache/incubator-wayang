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

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class ObjectFileSerializationTest {

    @Test
    public void jsonRoundtrip() throws Exception {
        Object[] chunk = new Object[]{"alpha", "beta", "gamma"};
        byte[] payload = ObjectFileSerialization.serializeChunk(
                chunk,
                chunk.length,
                ObjectFileSerializationMode.JSON);

        List<Object> result = ObjectFileSerialization.deserializeChunk(
                payload,
                ObjectFileSerializationMode.JSON,
                String.class);

        Assert.assertEquals(3, result.size());
        Assert.assertEquals("alpha", result.get(0));
        Assert.assertEquals("beta", result.get(1));
        Assert.assertEquals("gamma", result.get(2));
    }

    @Test
    public void legacyRoundtrip() throws Exception {
        SerializablePayload payload = new SerializablePayload("data", 42);
        Object[] chunk = new Object[]{payload};

        byte[] bytes = ObjectFileSerialization.serializeChunk(
                chunk,
                chunk.length,
                ObjectFileSerializationMode.LEGACY_JAVA_SERIALIZATION);

        List<Object> result = ObjectFileSerialization.deserializeChunk(
                bytes,
                ObjectFileSerializationMode.LEGACY_JAVA_SERIALIZATION,
                SerializablePayload.class);

        Assert.assertEquals(1, result.size());
        SerializablePayload deserialized = (SerializablePayload) result.get(0);
        Assert.assertEquals("data", deserialized.text);
        Assert.assertEquals(42, deserialized.value);
    }

    @Test(expected = java.io.InvalidClassException.class)
    public void legacyDeserializationRejectsDisallowedClass() throws Exception {
        java.util.concurrent.atomic.AtomicBoolean disallowed = new java.util.concurrent.atomic.AtomicBoolean(true);
        Object[] chunk = new Object[]{disallowed};

        byte[] bytes = ObjectFileSerialization.serializeChunk(
                chunk,
                chunk.length,
                ObjectFileSerializationMode.LEGACY_JAVA_SERIALIZATION);

        // Deserializing with an unrelated elementType (or null) should be rejected by the filter
        ObjectFileSerialization.deserializeChunk(
                bytes,
                ObjectFileSerializationMode.LEGACY_JAVA_SERIALIZATION,
                String.class);
    }

    @Test
    public void legacyDeserializationAllowedViaElementType() throws Exception {
        java.util.concurrent.atomic.AtomicBoolean payload = new java.util.concurrent.atomic.AtomicBoolean(true);
        Object[] chunk = new Object[]{payload};

        byte[] bytes = ObjectFileSerialization.serializeChunk(
                chunk,
                chunk.length,
                ObjectFileSerializationMode.LEGACY_JAVA_SERIALIZATION);

        // When elementType explicitly specifies AtomicBoolean, it should be permitted
        List<Object> result = ObjectFileSerialization.deserializeChunk(
                bytes,
                ObjectFileSerializationMode.LEGACY_JAVA_SERIALIZATION,
                java.util.concurrent.atomic.AtomicBoolean.class);

        Assert.assertEquals(1, result.size());
        Assert.assertTrue(((java.util.concurrent.atomic.AtomicBoolean) result.get(0)).get());
    }

    @Test
    public void legacyDeserializationAllowedViaSystemProperty() throws Exception {
        java.util.concurrent.atomic.AtomicBoolean payload = new java.util.concurrent.atomic.AtomicBoolean(true);
        Object[] chunk = new Object[]{payload};

        byte[] bytes = ObjectFileSerialization.serializeChunk(
                chunk,
                chunk.length,
                ObjectFileSerializationMode.LEGACY_JAVA_SERIALIZATION);

        String propKey = "wayang.basic.serialization.allowlist";
        String originalVal = System.getProperty(propKey);
        try {
            System.setProperty(propKey, "java.util.concurrent.atomic.**");
            List<Object> result = ObjectFileSerialization.deserializeChunk(
                    bytes,
                    ObjectFileSerializationMode.LEGACY_JAVA_SERIALIZATION,
                    String.class);
            Assert.assertEquals(1, result.size());
            Assert.assertTrue(((java.util.concurrent.atomic.AtomicBoolean) result.get(0)).get());
        } finally {
            if (originalVal != null) {
                System.setProperty(propKey, originalVal);
            } else {
                System.clearProperty(propKey);
            }
        }
    }

    @Test
    public void legacyDeserializationRejectsCustomNumberSubclass() throws Exception {
        CustomNumberPayload.hookExecuted = false;
        CustomNumberPayload payload = new CustomNumberPayload(123);
        byte[] bytes = ObjectFileSerialization.serializeChunk(
                new Object[]{payload}, 1, ObjectFileSerializationMode.LEGACY_JAVA_SERIALIZATION);

        try {
            ObjectFileSerialization.deserializeChunk(
                    bytes, ObjectFileSerializationMode.LEGACY_JAVA_SERIALIZATION, String.class);
            Assert.fail("Expected InvalidClassException");
        } catch (java.io.InvalidClassException e) {
            // Expected
        }
        Assert.assertFalse("Custom readObject() hook should not execute", CustomNumberPayload.hookExecuted);
    }

    @Test
    public void legacyDeserializationRejectsCustomCollectionSubclass() throws Exception {
        CustomCollectionPayload.hookExecuted = false;
        CustomCollectionPayload<String> payload = new CustomCollectionPayload<>();
        payload.add("item");
        byte[] bytes = ObjectFileSerialization.serializeChunk(
                new Object[]{payload}, 1, ObjectFileSerializationMode.LEGACY_JAVA_SERIALIZATION);

        try {
            ObjectFileSerialization.deserializeChunk(
                    bytes, ObjectFileSerializationMode.LEGACY_JAVA_SERIALIZATION, String.class);
            Assert.fail("Expected InvalidClassException");
        } catch (java.io.InvalidClassException e) {
            // Expected
        }
        Assert.assertFalse("Custom readObject() hook should not execute", CustomCollectionPayload.hookExecuted);
    }

    @Test
    public void legacyDeserializationRejectsCustomMapSubclass() throws Exception {
        CustomMapPayload.hookExecuted = false;
        CustomMapPayload<String, String> payload = new CustomMapPayload<>();
        payload.put("k", "v");
        byte[] bytes = ObjectFileSerialization.serializeChunk(
                new Object[]{payload}, 1, ObjectFileSerializationMode.LEGACY_JAVA_SERIALIZATION);

        try {
            ObjectFileSerialization.deserializeChunk(
                    bytes, ObjectFileSerializationMode.LEGACY_JAVA_SERIALIZATION, String.class);
            Assert.fail("Expected InvalidClassException");
        } catch (java.io.InvalidClassException e) {
            // Expected
        }
        Assert.assertFalse("Custom readObject() hook should not execute", CustomMapPayload.hookExecuted);
    }

    @Test
    public void legacyDeserializationAllowsImmutableCollections() throws Exception {
        List<String> list = List.of("alpha", "beta");
        java.util.Set<String> set = java.util.Set.of("gamma", "delta");
        java.util.Map<String, Integer> map = java.util.Map.of("key1", 10, "key2", 20);

        Object[] chunk = new Object[]{list, set, map};
        byte[] bytes = ObjectFileSerialization.serializeChunk(
                chunk, chunk.length, ObjectFileSerializationMode.LEGACY_JAVA_SERIALIZATION);

        List<Object> result = ObjectFileSerialization.deserializeChunk(
                bytes, ObjectFileSerializationMode.LEGACY_JAVA_SERIALIZATION, Object.class);

        Assert.assertEquals(3, result.size());
        Assert.assertEquals(list, result.get(0));
        Assert.assertEquals(set, result.get(1));
        Assert.assertEquals(map, result.get(2));
    }

    private static class SerializablePayload implements Serializable {
        final String text;
        final int value;

        private SerializablePayload(String text, int value) {
            this.text = text;
            this.value = value;
        }
    }

    public static class CustomNumberPayload extends Number implements Serializable {
        public static boolean hookExecuted = false;
        private final int val;

        public CustomNumberPayload(int val) {
            this.val = val;
        }

        @Override
        public int intValue() { return val; }
        @Override
        public long longValue() { return val; }
        @Override
        public float floatValue() { return val; }
        @Override
        public double doubleValue() { return val; }

        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            hookExecuted = true;
            in.defaultReadObject();
        }
    }

    public static class CustomCollectionPayload<E> extends java.util.ArrayList<E> implements Serializable {
        public static boolean hookExecuted = false;

        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            hookExecuted = true;
            in.defaultReadObject();
        }
    }

    public static class CustomMapPayload<K, V> extends java.util.HashMap<K, V> implements Serializable {
        public static boolean hookExecuted = false;

        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            hookExecuted = true;
            in.defaultReadObject();
        }
    }
}
