package org.qcri.rheem.profiler.log.sampling;

/**
 * Implementation of a battle between to elements.
 */
@FunctionalInterface
public interface Battle<T> {

    /**
     * Let to elements compete.
     *
     * @param t1 the first element
     * @param t2 the second element
     * @return the winner element, i.e., either {@code t1} or {@code t2}
     */
    T battle(T t1, T t2);

}
