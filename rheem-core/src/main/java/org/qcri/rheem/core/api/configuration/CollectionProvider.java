package org.qcri.rheem.core.api.configuration;

import org.qcri.rheem.core.api.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;

/**
 * Provides a {@link Collection} of objects.
 */
public abstract class CollectionProvider<Value> implements Iterable<Value> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected CollectionProvider<Value> parent;

    protected final Configuration configuration;

    public CollectionProvider(Configuration configuration) {
        this(configuration, null);
    }

    public CollectionProvider(Configuration configuration, CollectionProvider parent) {
        this.configuration = configuration;
        this.parent = parent;
    }


    public void setParent(CollectionProvider<Value> parent) {
        this.parent = parent;
    }

    /**
     * Provide all objects.
     *
     * @return the provided objects from this instance and its parents (unless those are masked).
     */
    public Collection<Value> provideAll() {
        return this.provideAll(this.configuration);
    }

    /**
     * Provide all objects.
     *
     * @return the provided objects from this instance and its parents (unless those are masked).
     */
    protected abstract Collection<Value> provideAll(Configuration configuration);

    @Override
    public Iterator<Value> iterator() {
        return this.provideAll().iterator();
    }
}
