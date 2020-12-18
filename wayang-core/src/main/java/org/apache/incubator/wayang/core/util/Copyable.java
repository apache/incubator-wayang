package org.apache.incubator.wayang.core.util;

/**
 * Implementing objects must be able to provide copies of themselves.
 */
public interface Copyable<Self> {

    /**
     * Create a (potentially shallow) copy of this instance.
     *
     * @return the copy
     */
    Self copy();

}
