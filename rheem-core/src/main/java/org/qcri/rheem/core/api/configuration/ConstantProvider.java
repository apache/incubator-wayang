package org.qcri.rheem.core.api.configuration;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Used by {@link Configuration}s to provide some value.
 */
public class ConstantProvider<Value> {

    public static class NotAvailableException extends RheemException {
        public NotAvailableException(String message) {
            super(message);
        }
    }

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected ConstantProvider<Value> parent;

    private Value value;

    private String warningSlf4j;

    public ConstantProvider(ConstantProvider<Value> parent) {
        this(parent, null);
    }

    public ConstantProvider(Value value) {
        this(null, value);
    }

    public ConstantProvider(ConstantProvider<Value> parent, Value value) {
        this.parent = parent;
        this.value = value;
    }

    public Value provide() {
        return this.provide(this);
    }

    protected Value provide(ConstantProvider<Value> requestee) {
        if (this.warningSlf4j != null) {
            this.logger.warn(this.warningSlf4j);
        }

        // Look for a custom answer.
        if (this.value != null) {
            return this.value;
        }

        // If present, delegate request to parent.
        if (this.parent != null) {
            return this.parent.provide(requestee);
        }

        throw new NotAvailableException(String.format("Could not provide value."));
    }

    public Optional<Value> optionallyProvide() {
        try {
            return Optional.of(this.provide());
        } catch (NotAvailableException nske) {
            return Optional.empty();
        }
    }



    public void setParent(ConstantProvider<Value> parent) {
        this.parent = parent;
    }

    public ConstantProvider<Value> withSlf4jWarning(String message) {
        this.warningSlf4j = message;
        return this;
    }

    public void set(Value value) {
        this.value = value;
    }
}
