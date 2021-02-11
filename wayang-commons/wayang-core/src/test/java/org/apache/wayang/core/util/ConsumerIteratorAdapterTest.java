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

import org.junit.Assert;
import org.junit.Test;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Test suite for the {@link ConsumerIteratorAdapter}.
 */
public class ConsumerIteratorAdapterTest {

    @Test
    public void testCriticalLoad() {
        final int maxI = 50000000;

        final ConsumerIteratorAdapter<Integer> adapter = new ConsumerIteratorAdapter<>(maxI / 1000);
        final Iterator<Integer> iterator = adapter.getIterator();
        final Consumer<Integer> consumer = adapter.getConsumer();

        Thread producerThread = new Thread(
                () -> {
                    for (int i = 0; i < maxI; i++) {
                        consumer.accept(i);
                    }
                    adapter.declareLastAdd();
                }
        );
        producerThread.start();

        Logger logger = LogManager.getLogger(this.getClass());
        for (int i = 0; i < maxI; i++) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(i, iterator.next().intValue());
            if (i > 0 && i % 10000000 == 0) logger.info("Put through {} elements.", i);
        }
        Assert.assertFalse(iterator.hasNext());
    }


}
