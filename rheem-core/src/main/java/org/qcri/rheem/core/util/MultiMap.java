package org.qcri.rheem.core.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Maps keys to multiple values.
 */
public class MultiMap<K, V> {

    private Map<K, Set<V>> map = new HashMap<>();

    public Set<V> put(K key, V value) {
        final Set<V> values = this.map.computeIfAbsent(key, k -> new HashSet<>());
        values.add(value);
        return values;
    }

    public Set<V> remove(K key) {
        return this.map.remove(key);
    }

    public Set<V> remove(K key, V value) {
        final Set<V> values = this.map.get(key);
        if (values != null) {
            values.remove(value);
        }
        return values;
    }

}
