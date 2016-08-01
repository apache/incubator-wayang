package org.qcri.rheem.core.api.configuration;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Used by {@link Configuration}s to provide some value.
 */
public abstract class ValueProvider<Value> {

    public static class NotAvailableException extends RheemException {
        public NotAvailableException(String message) {
            super(message);
        }
    }

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Configuration configuration;

    protected final ValueProvider<Value> parent;

    private String warningSlf4j;

    protected ValueProvider(Configuration configuration, ValueProvider<Value> parent) {
        this.parent = parent;
        this.configuration = configuration;
    }

    public Value provide() {
        return this.provide(this);
    }

    protected Value provide(ValueProvider<Value> requestee) {
        if (this.warningSlf4j != null) {
            this.logger.warn(this.warningSlf4j);
        }

        // Look for a custom answer.
        Value value = this.tryProvide(requestee);
        if (value != null) {
            return value;
        }

        // If present, delegate request to parent.
        if (this.parent != null) {
            return this.parent.provide(requestee);
        }

        throw new NotAvailableException(String.format("Could not provide value."));
    }

    /**
     * Try to provide a value.
     *
     * @param requestee the original instance asked to provide a value
     * @return the requested value or {@code null} if the request could not be served
     */
    protected abstract Value tryProvide(ValueProvider<Value> requestee);

    public Optional<Value> optionallyProvide() {
        try {
            return Optional.of(this.provide());
        } catch (NotAvailableException nske) {
            return Optional.empty();
        }
    }

    public ValueProvider<Value> withSlf4jWarning(String message) {
        this.warningSlf4j = message;
        return this;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }
}
