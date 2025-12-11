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

    private static class SerializablePayload implements Serializable {
        final String text;
        final int value;

        private SerializablePayload(String text, int value) {
            this.text = text;
            this.value = value;
        }
    }
}
