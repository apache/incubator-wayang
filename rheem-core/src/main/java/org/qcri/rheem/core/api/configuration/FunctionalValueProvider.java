package org.qcri.rheem.core.api.configuration;

import org.qcri.rheem.core.api.Configuration;

import java.util.function.Function;

/**
 * Used by {@link Configuration}s to provide some value.
 */
public class FunctionalValueProvider<Value> extends ValueProvider<Value> {

    private final Function<ValueProvider<Value>, Value> valueFunction;

    public FunctionalValueProvider(Function<ValueProvider<Value>, Value> valueFunction, Configuration configuration) {
        this(valueFunction, configuration, null);
    }

    public FunctionalValueProvider(Function<ValueProvider<Value>, Value> valueFunction, ValueProvider<Value> parent) {
        this(valueFunction, parent.getConfiguration(), parent);
    }

    public FunctionalValueProvider(Function<ValueProvider<Value>, Value> valueFunction, Configuration configuration, ValueProvider<Value> parent) {
        super(configuration, parent);
        this.valueFunction = valueFunction;
    }

    @Override
    protected Value tryProvide(ValueProvider<Value> requestee) {
        return this.valueFunction.apply(requestee);
    }

}
