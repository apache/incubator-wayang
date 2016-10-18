package org.qcri.rheem.core.util;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        Logger logger = LoggerFactory.getLogger(this.getClass());
        for (int i = 0; i < maxI; i++) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(i, iterator.next().intValue());
            if (i > 0 && i % 10000000 == 0) logger.info("Put through {} elements.", i);
        }
        Assert.assertFalse(iterator.hasNext());
    }


}