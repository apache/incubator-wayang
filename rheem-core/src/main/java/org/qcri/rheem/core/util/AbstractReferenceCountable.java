package org.qcri.rheem.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a template for {@link ReferenceCountable} objects.
 */
public abstract class AbstractReferenceCountable implements ReferenceCountable {

    private static final Logger logger = LoggerFactory.getLogger(ReferenceCountable.class);

    /**
     * Maintains the number of references on this object.
     */
    private int numReferences = 0;

    /**
     * Marks whether this instance has been disposed to avoid unwanted resurrection, double disposal etc.
     *
     * @see #disposeIfUnreferenced()
     */
    private boolean isDisposed = false;

    @Override
    public boolean disposeIfUnreferenced() {
        if (this.getNumReferences() == 0) {
            assert !this.isDisposed() : String.format("%s has already been disposed.", this);
            logger.debug("Discarding {} for being unreferenced.", this);
            this.disposeUnreferenced();
            this.isDisposed = true;
            return true;
        }
        return false;
    }

    /**
     * Dispose this instance, which is not referenced anymore. This method should always be invoked through
     * {@link #disposeUnreferenced()}
     */
    protected abstract void disposeUnreferenced();

    @Override
    public int getNumReferences() {
        return this.numReferences;
    }

    @Override
    public void noteObtainedReference() {
        assert !this.isDisposed() : String.format("%s should not be resurrected.", this);
        this.numReferences++;
        logger.trace("{} has {} (+1) references now.", this, this.getNumReferences());
    }

    @Override
    public void noteDiscardedReference(boolean isDisposeIfUnreferenced) {
        assert this.numReferences > 0 : String.format("Reference on %s discarded, although the reference counter is 0.", this);
        this.numReferences--;
        logger.trace("{} has {} (-1) references now.", this, this.getNumReferences());
        if (isDisposeIfUnreferenced) {
            this.disposeIfUnreferenced();
        }
    }

    @Override
    public boolean isDisposed() {
        return this.isDisposed;
    }

}
