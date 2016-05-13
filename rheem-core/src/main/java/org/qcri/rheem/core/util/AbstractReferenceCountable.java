package org.qcri.rheem.core.util;

import org.slf4j.LoggerFactory;

/**
 * Implements a template for {@link ReferenceCountable} objects.
 */
public abstract class AbstractReferenceCountable implements ReferenceCountable {

    /**
     * Maintains the number of references on this object.
     */
    private int numReferences = 0;

    @Override
    public boolean disposeIfUnreferenced() {
        if (this.getNumReferences() == 0) {
            LoggerFactory.getLogger(this.getClass()).info("Discarding {} for being unreferenced.", this);
            this.disposeUnreferenced();
            return true;
        }
        return false;
    }

    /**
     * Dispose this instance, which is not referenced anymore.
     */
    protected abstract void disposeUnreferenced();

    @Override
    public int getNumReferences() {
        return this.numReferences;
    }

    @Override
    public void noteObtainedReference() {
        this.numReferences++;
        LoggerFactory.getLogger(this.getClass()).info("{} has {} (+1) references now.", this, this.getNumReferences());
    }

    @Override
    public void noteDiscardedReference(boolean isDisposeIfUnreferenced) {
        assert this.numReferences > 0;
        this.numReferences--;
        LoggerFactory.getLogger(this.getClass()).info("{} has {} (-1) references now.", this, this.getNumReferences());
        if (isDisposeIfUnreferenced) {
            this.disposeIfUnreferenced();
        }
    }
}
