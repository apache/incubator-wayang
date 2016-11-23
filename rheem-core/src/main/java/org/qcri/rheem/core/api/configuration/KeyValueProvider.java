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

    protected final Configuration configuration;

    private String warningSlf4jFormat;

    protected KeyValueProvider(KeyValueProvider<Key, Value> parent) {
        this(parent, parent.configuration);
    }

    protected KeyValueProvider(KeyValueProvider<Key, Value> parent, Configuration configuration) {
        this.parent = parent;
        this.configuration = configuration;
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
            final Value parentValue = this.parent.provideFor(key, requestee);
            this.processParentEntry(key, parentValue);
            return parentValue;
        }

        throw new NoSuchKeyException(String.format("Could not provide value for %s from %s.", key, requestee.getConfiguration()));
    }

    public Optional<Value> optionallyProvideFor(Key key) {
        try {
            return Optional.of(this.provideFor(key));
        } catch (NoSuchKeyException nske) {
            return Optional.empty();
        }
    }

    /**
     * Provide the appropriate value for the {@code key}.
     *
     * @return the value of {@code null} if none could be provided
     */
    protected abstract Value tryToProvide(Key key, KeyValueProvider<Key, Value> requestee);

    /**
     * Provide the value associated to the given key from this very instance, i.e., do not relay the request
     * to parent instances on misses.
     *
     * @param key the key
     * @return the value or {@code null} if this very instance does not associate a value with the key
     */
    public Value provideLocally(Key key) {
        return this.tryToProvide(key, this);
    }

    /**
     * React to a value that has been delivered by a {@link #parent}. Does nothing be default.
     *
     * @param key   the key of the entry
     * @param value the value of the entry or {@code null} if no such value exists
     */
    protected void processParentEntry(Key key, Value value) {
    }

    public void setParent(KeyValueProvider<Key, Value> parent) {
        this.parent = parent;
    }


    /**
     * Log a warning in SLF4J format when using this instance to provide a value. The requested key will be passed
     * as parameter.
     *
     * @return this instance
     */
    public KeyValueProvider<Key, Value> withSlf4jWarning(String slf4jFormat) {
        this.warningSlf4jFormat = slf4jFormat;
        return this;
    }

    public void set(Key key, Value value) {
        throw new RheemException(String.format("Setting values not supported for %s.", this.getClass().getSimpleName()));
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }
}
