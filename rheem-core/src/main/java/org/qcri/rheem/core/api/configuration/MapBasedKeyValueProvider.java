package org.qcri.rheem.core.api.configuration;

import org.apache.commons.lang3.Validate;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link KeyValueProvider} that uses a {@link Map} to provide a value.
 */
public class MapBasedKeyValueProvider<Key, Value> extends KeyValueProvider<Key, Value> {

    private final Map<Key, Value> storedValues = new HashMap<>();

    private final boolean isCaching;

    /**
     * Creates a new caching instance.
     */
    public MapBasedKeyValueProvider(KeyValueProvider<Key, Value> parent) {
        this(parent, true);
    }


    /**
     * Creates a new instance.
     *
     * @param isCaching if, when a value is provided by the {@code parent}, that value should be cached in the new instance
     */
    public MapBasedKeyValueProvider(KeyValueProvider<Key, Value> parent, boolean isCaching) {
        super(parent);
        this.isCaching = isCaching;
    }

    @Override
    public Value provideFor(Key key) {
        final Value value = super.provideFor(key);
        if (this.isCaching && value != null) {
            this.storedValues.putIfAbsent(key, value);
        }
        return value;
    }

    @Override
    public Value tryToProvide(Key key, KeyValueProvider<Key, Value> requestee) {
        Validate.notNull(key);
        return this.storedValues.get(key);
    }

    @Override
    public void set(Key key, Value value) {
        Validate.notNull(key);
        this.storedValues.put(key, value);
    }

}
