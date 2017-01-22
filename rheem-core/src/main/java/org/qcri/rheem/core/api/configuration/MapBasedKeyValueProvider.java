package org.qcri.rheem.core.api.configuration;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;

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
     * Creates a new caching instance.
     */
    public MapBasedKeyValueProvider(KeyValueProvider<Key, Value> parent, Configuration configuration) {
        this(parent, configuration, true);
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


    /**
     * Creates a new instance.
     *
     * @param isCaching if, when a value is provided by the {@code parent}, that value should be cached in the new instance
     */
    public MapBasedKeyValueProvider(Configuration configuration, boolean isCaching) {
        super(null, configuration);
        this.isCaching = isCaching;
    }

    /**
     * Creates a new instance.
     */
    public MapBasedKeyValueProvider(KeyValueProvider<Key, Value> parent, Configuration configuration, boolean isCaching) {
        super(parent, configuration);
        this.isCaching = isCaching;
    }

    @Override
    public Value tryToProvide(Key key, KeyValueProvider<Key, Value> requestee) {
        Validate.notNull(key);
        return this.storedValues.get(key);
    }

    @Override
    protected void processParentEntry(Key key, Value value) {
        super.processParentEntry(key, value);
        if (this.isCaching && value != null) {
            this.storedValues.putIfAbsent(key, value);
        }
    }

    @Override
    public void set(Key key, Value value) {
        Validate.notNull(key);
        this.storedValues.put(key, value);
    }

}
