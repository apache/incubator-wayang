package org.qcri.rheem.core.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Maps keys to multiple values.
 */
public class MultiMap<K, V> extends HashMap<K, Set<V>> {

    public Set<V> putSingle(K key, V value) {
        final Set<V> values = this.computeIfAbsent(key, k -> new HashSet<>());
        values.add(value);
        return values;
    }

    public Set<V> removeSingle(K key, V value) {
        final Set<V> values = this.get(key);
        if (values != null) {
            values.remove(value);
        }
        return values;
    }

}
