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

import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.types.DataSetType;

/**
 * Tests for the {@link RecordType}.
 */
public class RecordTypeTest {

    @Test
    public void testSupertype() {
        DataSetType<Record> t1 = DataSetType.createDefault(Record.class);
        DataSetType<Record> t2 = DataSetType.createDefault(new RecordType("a", "b"));
        DataSetType<Record> t3 = DataSetType.createDefault(new RecordType("a", "b", "c"));

        Assert.assertTrue(t1.isSupertypeOf(t2));
        Assert.assertFalse(t2.isSupertypeOf(t1));
        Assert.assertTrue(t1.isSupertypeOf(t3));
        Assert.assertFalse(t3.isSupertypeOf(t1));
        Assert.assertTrue(t2.isSupertypeOf(t2));
        Assert.assertFalse(t2.isSupertypeOf(t3));
        Assert.assertTrue(t3.isSupertypeOf(t3));
        Assert.assertFalse(t3.isSupertypeOf(t2));
    }
}
