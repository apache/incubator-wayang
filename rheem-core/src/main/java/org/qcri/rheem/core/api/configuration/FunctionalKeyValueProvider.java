package org.qcri.rheem.core.api.configuration;

import org.qcri.rheem.core.api.Configuration;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Implementation of {@link KeyValueProvider} that uses a {@link Function} to provide a value.
 */
public class FunctionalKeyValueProvider<Key, Value> extends KeyValueProvider<Key, Value> {

    private final BiFunction<Key, KeyValueProvider<Key, Value>, Value> providerFunction;

    public FunctionalKeyValueProvider(Function<Key, Value> providerFunction, Configuration configuration) {
        this(null, configuration, uncur(providerFunction));
    }

    public FunctionalKeyValueProvider(KeyValueProvider<Key, Value> parent, Function<Key, Value> providerFunction) {
        this(parent, parent.configuration, uncur(providerFunction));
    }

    public FunctionalKeyValueProvider(BiFunction<Key, KeyValueProvider<Key, Value>, Value> providerFunction,
                                      Configuration configuration) {
        this(null, configuration, providerFunction);
    }
    public FunctionalKeyValueProvider(KeyValueProvider<Key, Value> parent,
                                      BiFunction<Key, KeyValueProvider<Key, Value>, Value> providerFunction) {
        this(parent, parent.configuration, providerFunction);
    }

    private FunctionalKeyValueProvider(KeyValueProvider<Key, Value> parent,
                                      Configuration configuration,
                                      BiFunction<Key, KeyValueProvider<Key, Value>, Value> providerFunction) {
        super(parent, configuration);
        this.providerFunction = providerFunction;
    }

    private static <Key, Value> BiFunction<Key, KeyValueProvider<Key, Value>, Value> uncur(Function<Key, Value> function) {
        return (key, provider) -> function.apply(key);
    }

    @Override
    protected Value tryToProvide(Key key, KeyValueProvider<Key, Value> requestee) {
        return this.providerFunction.apply(key, requestee);
    }

}
