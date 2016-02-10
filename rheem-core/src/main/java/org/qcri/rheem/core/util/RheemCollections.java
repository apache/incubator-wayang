package org.qcri.rheem.core.util;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

/**
 * Utilities to operate {@link java.util.Collection}s.
 */
public class RheemCollections {

    private RheemCollections() { }

    public static <K, V> void put(Map<K, Collection<V>> map, K key, V value) {
        map.compute(key, (k, values) -> {
            if (values == null) {
                values = new LinkedList<>();
            }
            values.add(value);
            return values;
        });
    }

}
