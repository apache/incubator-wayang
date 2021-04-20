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
package org.apache.wayang.plugin.hackit.core.identifiers.generator;

import org.apache.wayang.plugin.hackit.core.identifiers.HackitIDGenerator;

import java.time.Instant;

/**
 * SnowflackID is and implementation of <a href="https://en.wikipedia.org/wiki/Snowflake_ID">this</a>
 */
public class SnowflakeID extends HackitIDGenerator<Integer, Long> {
    private static final int TOTAL_BITS = 64;
    private static final int EPOCH_BITS = 42;
    private static final int NODE_ID_BITS = 10;
    private static final int SEQUENCE_BITS = 12;

    private static final int maxNodeId = (int)(Math.pow(2, NODE_ID_BITS) - 1);
    private static final int maxSequence = (int)(Math.pow(2, SEQUENCE_BITS) - 1);

    // Custom Epoch (January 1, 2015 Midnight UTC = 2015-01-01T00:00:00Z)
    //TODO: add this element from configuration
    private static final long CUSTOM_EPOCH = 1420070400000L;

    private volatile long lastTimestamp = -1L;
    private volatile long sequence = 0L;

    /**
     * Create SequenceGenerator with a nodeId
     */
    public SnowflakeID(int nodeId) {
        if(nodeId < 0 || nodeId > maxNodeId) {
            throw new IllegalArgumentException(String.format("NodeId must be between %d and %d", 0, maxNodeId));
        }
        this.identify_process = nodeId;
    }

    /**
     * Let SequenceGenerator generate a nodeId
     */
    public SnowflakeID() {
        this( createNodeId() & maxNodeId);
    }

    @Override
    public Long generateId() {
        return this.nextId();
    }

    /**
     * Generate the next ID, this method is synchronized because several {@link Thread} could exist
     * on one unique worker.
     *
     * @return the new ID
     */
    public synchronized long nextId() {
        long currentTimestamp = timestamp();

        if(currentTimestamp < lastTimestamp) {
            throw new IllegalStateException("Invalid System Clock!");
        }

        if (currentTimestamp == lastTimestamp) {
            sequence = (sequence + 1) & maxSequence;
            if(sequence == 0) {
                // Sequence Exhausted, wait till next millisecond.
                currentTimestamp = waitNextMillis(currentTimestamp);
            }
        } else {
            // reset sequence to start with zero for the next millisecond
            sequence = 0;
        }

        lastTimestamp = currentTimestamp;

        long id = currentTimestamp << (TOTAL_BITS - EPOCH_BITS);
        id |= (this.identify_process << (TOTAL_BITS - EPOCH_BITS - NODE_ID_BITS));
        id |= sequence;
        return id;
    }

    /**
     * Get current timestamp in milliseconds, adjust for the custom epoch.
     *
     * @return the Timestamp
     */
    private static long timestamp() {
        return Instant.now().toEpochMilli() - CUSTOM_EPOCH;
    }

    /**
     * Block and wait till next millisecond, this is used when the number of elements of one epoch
     * overflow the max possible number of one epoch
     *
     * @param currentTimestamp
     *
     * @return the new timestamp after the waiting time
     */
    private long waitNextMillis(long currentTimestamp) {
        while (currentTimestamp == lastTimestamp) {
            currentTimestamp = timestamp();
        }
        return currentTimestamp;
    }
}
