package org.qcri.rheem.core.util;

/**
 * This interface provides a reference-counting scheme, e.g., to manage allocated external resources. The initial
 * number of references after the object instantiation is always {@code 0}.
 */
public interface ReferenceCountable {

    /**
     * Tells the number of references on this instance.
     *
     * @return the number of references
     */
    int getNumReferences();

    /**
     * Declare that there is a new reference obtained on this instance.
     */
    void noteObtainedReference();

    /**
     * Declare that a reference on this instance has been discarded. Optionally, dispose this instance if there are
     * no remaining references.
     *
     * @param isDisposeIfUnreferenced whether to dispose this instance if there are no more references
     */
    void noteDiscardedReference(boolean isDisposeIfUnreferenced);

    /**
     * Dispose this instance if there are no more references to it.
     *
     * @return whether this instance is not referenced any more
     */
    boolean disposeIfUnreferenced();
}
