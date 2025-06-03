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

package org.apache.wayang.basic.types;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.types.DataSetType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the {@link RecordType}.
 */
class RecordTypeTest {

    @Test
    void testSupertype() {
        DataSetType<Record> t1 = DataSetType.createDefault(Record.class);
        DataSetType<Record> t2 = DataSetType.createDefault(new RecordType("a", "b"));
        DataSetType<Record> t3 = DataSetType.createDefault(new RecordType("a", "b", "c"));

        assertTrue(t1.isSupertypeOf(t2));
        assertFalse(t2.isSupertypeOf(t1));
        assertTrue(t1.isSupertypeOf(t3));
        assertFalse(t3.isSupertypeOf(t1));
        assertTrue(t2.isSupertypeOf(t2));
        assertFalse(t2.isSupertypeOf(t3));
        assertTrue(t3.isSupertypeOf(t3));
        assertFalse(t3.isSupertypeOf(t2));
    }
}
