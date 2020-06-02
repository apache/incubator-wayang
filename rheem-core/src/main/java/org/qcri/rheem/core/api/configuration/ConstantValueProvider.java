package org.qcri.rheem.core.api.configuration;

import org.qcri.rheem.core.api.Configuration;

/**
 * Used by {@link Configuration}s to provide some value.
 */
public class ConstantValueProvider<Value> extends ValueProvider<Value> {

    private Value value;

    public ConstantValueProvider(Value value, Configuration configuration) {
        this(configuration, null);
        this.setValue(value);
    }

    public ConstantValueProvider(Configuration configuration) {
        this(configuration, null);
    }

    public ConstantValueProvider(ValueProvider<Value> parent) {
        this(parent.getConfiguration(), parent);
    }

    public ConstantValueProvider(Configuration configuration, ValueProvider<Value> parent) {
        super(configuration, parent);
    }

    public void setValue(Value value) {
        this.value = value;
    }

    @Override
    protected Value tryProvide(ValueProvider<Value> requestee) {
        return this.value;
    }

}
