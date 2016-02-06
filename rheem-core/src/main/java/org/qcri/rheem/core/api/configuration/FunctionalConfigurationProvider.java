package org.qcri.rheem.core.api.configuration;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Implementation of {@link ConfigurationProvider} that uses a {@link Function} to provide a value.
 */
public class FunctionalConfigurationProvider<Key, Value> extends ConfigurationProvider<Key, Value> {

    private final BiFunction<Key, ConfigurationProvider<Key, Value>, Value> providerFunction;

    public FunctionalConfigurationProvider(Function<Key, Value> providerFunction) {
        this(null, providerFunction);
    }

    public FunctionalConfigurationProvider(ConfigurationProvider<Key, Value> parent, Function<Key, Value> providerFunction) {
        this(parent, uncur(providerFunction));
    }

    public FunctionalConfigurationProvider(BiFunction<Key, ConfigurationProvider<Key, Value>, Value> providerFunction) {
        this(null, providerFunction);
    }

    public FunctionalConfigurationProvider(ConfigurationProvider<Key, Value> parent,
                                           BiFunction<Key, ConfigurationProvider<Key, Value>, Value> providerFunction) {
        super(parent);
        this.providerFunction = providerFunction;
    }

    private static <Key, Value> BiFunction<Key, ConfigurationProvider<Key, Value>, Value> uncur(Function<Key, Value> function) {
        return (key, provider) -> function.apply(key);
    }

    @Override
    protected Value tryToProvide(Key key, ConfigurationProvider<Key, Value> requestee) {
        return this.providerFunction.apply(key, requestee);
    }

}
