package org.qcri.rheem.core.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Iterates all combinations, i.e., the Cartesian product, of given {@link Iterable}s.
 */
public class CrossProductIterable<T> implements Iterable<List<T>> {

    public final List<? extends Iterable<T>> iterables;

    public CrossProductIterable(List<? extends Iterable<T>> iterables) {
        this.iterables = new ArrayList<>(iterables);
    }

    @Override
    public java.util.Iterator<List<T>> iterator() {
        return new CrossProductIterable.Iterator<>(this);
    }

    private static class Iterator<T> implements java.util.Iterator<List<T>> {

        private final CrossProductIterable<T> crossProductIterable;

        private final List<java.util.Iterator<T>> partialIterators;

        private List<T> vals;

        private boolean hasNext;

        private Iterator(CrossProductIterable<T> crossProductIterable) {
            // Initialize.
            this.crossProductIterable = crossProductIterable;
            this.partialIterators = new ArrayList<>(this.crossProductIterable.iterables.size());
            this.vals = new ArrayList<>(this.crossProductIterable.iterables.size());

            this.hasNext = true;
            for (Iterable<T> iterable : this.crossProductIterable.iterables) {
                final java.util.Iterator<T> iterator = iterable.iterator();
                this.partialIterators.add(iterator);
                this.vals.add(null);
                if (!(this.hasNext &= iterator.hasNext())) {
                    return;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return this.hasNext;
        }

        @Override
        public List<T> next() {
            List<T> next = new ArrayList<>(this.partialIterators.size());
            this.hasNext = false;
            boolean isFetchNext = true;
            for (int i = 0; i < this.partialIterators.size(); i++) {
                java.util.Iterator<T> partialIterator = this.partialIterators.get(i);
                if (isFetchNext) {
                    // If the Iterator has made a full pass, replace it with a new one.
                    if (!partialIterator.hasNext()) {
                        partialIterator = this.crossProductIterable.iterables.get(i).iterator();
                        this.partialIterators.set(i, partialIterator);
                        this.vals.set(i, null);
                        assert partialIterator.hasNext();
                    }

                    // If the Iterator had made a full pass or this is the very first iteration.
                    if (this.vals.get(i) == null) {
                        this.vals.set(i, partialIterator.next());
                    } else {
                        isFetchNext = false;
                    }
                }

                next.add(this.vals.get(i));
                this.hasNext |= partialIterator.hasNext();
            }

            return next;
        }
    }
}
