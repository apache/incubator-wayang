package org.qcri.rheem.core.api.configuration;

import org.apache.commons.lang3.Validate;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link ConfigurationProvider} that uses a {@link Map} to provide a value.
 */
public class MapBasedConfigurationProvider<Key, Value> extends ConfigurationProvider<Key, Value> {

    private final Map<Key, Value> storedValues = new HashMap<>();

    public MapBasedConfigurationProvider(ConfigurationProvider<Key, Value> parent) {
        super(parent);
    }

    @Override
    public Value tryToProvide(Key key, ConfigurationProvider<Key, Value> requestee) {
        Validate.notNull(key);
        return this.storedValues.get(key);
    }

    @Override
    public void set(Key key, Value value) {
        Validate.notNull(key);
        this.storedValues.put(key, value);
    }

}
