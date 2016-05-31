package org.qcri.rheem.core.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Maps keys to multiple values. Each key value pair is unique.
 */
public class MultiMap<K, V> extends HashMap<K, Set<V>> {

    /**
     * Associate a key with a new value.
     *
     * @param key   to associate with
     * @param value will be associated
     * @return whether the value was not yet associated with the key
     */
    public boolean putSingle(K key, V value) {
        final Set<V> values = this.computeIfAbsent(key, k -> new HashSet<>());
        return values.add(value);
    }


    /**
     * Disassociate a key with a value.
     *
     * @param key   to disassociate from
     * @param value will be disassociated
     * @return whether the value was associated with the key
     */
    public boolean removeSingle(K key, V value) {
        final Set<V> values = this.get(key);
        if (values != null) {
            return values.remove(value);
        }
        return false;
    }

}
