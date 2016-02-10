package org.qcri.rheem.core.api.configuration;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Used by {@link Configuration}s to provide some value.
 */
public abstract class KeyValueProvider<Key, Value> {

    public static class NoSuchKeyException extends RheemException {
        public NoSuchKeyException(String message) {
            super(message);
        }
    }

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected KeyValueProvider<Key, Value> parent;

    private String warningSlf4jFormat;

    protected KeyValueProvider(KeyValueProvider<Key, Value> parent) {
        this.parent = parent;
    }

    public Value provideFor(Key key) {
        return this.provideFor(key, this);
    }

    protected Value provideFor(Key key, KeyValueProvider<Key, Value> requestee) {
        if (this.warningSlf4jFormat != null) {
            this.logger.warn(this.warningSlf4jFormat, key);
        }

        // Look for a custom answer.
        final Value value = this.tryToProvide(key, requestee);
        if (value != null) {
            return value;
        }

        // If present, delegate request to parent.
        if (this.parent != null) {
            return this.parent.provideFor(key, requestee);
        }

        throw new NoSuchKeyException(String.format("Could not provide value for %s.", key));
    }

    public Optional<Value> optionallyProvideFor(Key key) {
        try {
            return Optional.of(this.provideFor(key));
        } catch (NoSuchKeyException nske) {
            return Optional.empty();
        }
    }

    protected abstract Value tryToProvide(Key key, KeyValueProvider<Key, Value> requestee);


    public void setParent(KeyValueProvider<Key, Value> parent) {
        this.parent = parent;
    }

    public KeyValueProvider<Key, Value> withSlf4jWarning(String slf4jFormat) {
        this.warningSlf4jFormat = slf4jFormat;
        return this;
    }

    public void set(Key key, Value value) {
        throw new RheemException(String.format("Setting values not supported for %s.", this.getClass().getSimpleName()));
    }
}
