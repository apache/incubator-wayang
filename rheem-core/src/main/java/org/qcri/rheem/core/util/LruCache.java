package org.qcri.rheem.core.util;

import org.apache.commons.lang3.Validate;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Key-value cache with "least recently used" eviction strategy.
 */
public class LruCache<K, V> extends LinkedHashMap<K, V> {

    private final int capacity;

    public LruCache(int capacity) {
        Validate.isTrue(capacity > 0);
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return this.size() > this.capacity;
    }

    public int getCapacity() {
        return this.capacity;
    }
}
