package org.qcri.rheem.core.api.configuration;

import org.qcri.rheem.core.api.Configuration;

import java.util.Collection;
import java.util.HashSet;
import java.util.function.Function;

/**
 * {@link CollectionProvider} implementation based on a blacklist and a whitelist.
 */
public class FunctionalCollectionProvider<Value> extends CollectionProvider<Value> {

    private final Function<Configuration, Collection<Value>> providerFunction;

    public FunctionalCollectionProvider(Function<Configuration, Collection<Value>> providerFunction, Configuration configuration) {
        super(configuration);
        this.providerFunction = providerFunction;
    }

    public FunctionalCollectionProvider(Function<Configuration, Collection<Value>> providerFunction,
                                        Configuration configuration,
                                        CollectionProvider parent) {
        super(configuration, parent);
        this.providerFunction = providerFunction;
    }

    @Override
    public Collection<Value> provideAll(Configuration configuration) {
        Collection<Value> values = this.parent == null ? new HashSet<>() : new HashSet<>(this.parent.provideAll(configuration));
        values.addAll(this.providerFunction.apply(configuration));
        return values;
    }

}
