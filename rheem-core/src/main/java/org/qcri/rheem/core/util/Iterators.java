package org.qcri.rheem.core.util;

import java.util.Iterator;
import java.util.function.Predicate;

/**
 * Utilities for the work with {@link Iterator}s.
 */
public class Iterators {

    /**
     * Test the a given {@link Predicate} on all remaining elements of an {@link Iterator} and return whether it always
     * evaluated to {@code true}.
     *
     * @param iterator     that should be inspected
     * @param predicate    that should be applied to the elements of the {@code iterator}
     * @param isAbortEarly whether search can be aborted on the first sight of {@code false}
     * @param <T>          type of the elements
     * @return whether all tests returned {@code true}
     */
    public static <T> boolean allMatch(Iterator<T> iterator, Predicate<T> predicate, boolean isAbortEarly) {
        boolean result = true;
        while (iterator.hasNext()) {
            result &= predicate.test(iterator.next());
            if (!result && isAbortEarly) {
                return false;
            }
        }
        return result;
    }

    /**
     * Wrap the given {@link Iterator} in an {@link Iterable}.
     *
     * @param iterator the {@link Iterator}
     * @return the {@link Iterable}
     */
    public static <T> Iterable<T> wrapWithIterable(Iterator<T> iterator) {
        return () -> iterator;
    }
}