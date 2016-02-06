package org.qcri.rheem.core.util;

import java.util.stream.Stream;

/**
 * Utility similar to {@link java.util.Optional}. However, it supports {@code null} as a valid value.
 */
public abstract class Optional<T> {

    /**
     * @return whether this instance contains a value.
     */
    public abstract boolean isAvailable();

    /**
     * @return the value contained by this instance
     * @throws IllegalStateException if {@link #isAvailable()} is {@code false}
     */
    public abstract T getValue();

    /**
     * @return a {@link Stream} containing the optional value
     */
    public abstract Stream<T> stream();

    private static final Optional NA = new Optional() {
        @Override
        public boolean isAvailable() {
            return false;
        }

        @Override
        public Object getValue() {
            throw new IllegalStateException("No value available.");
        }

        @Override
        public Stream stream() {
            return Stream.empty();
        }
    };

    /**
     * Create a n/a instance.
     *
     * @return an {@link Optional} without a value
     */
    @SuppressWarnings("unchecked")
    public static <T> Optional<T> na() {
        return NA;
    }

    /**
     * Create a new instance with the given {@code value}.
     *
     * @param value the value of the new {@link Optional}
     * @return the new instance
     */
    public static <T> Optional<T> of(final T value) {
        return new Optional<T>() {
            @Override
            public boolean isAvailable() {
                return true;
            }

            @Override
            public T getValue() {
                return value;
            }

            @Override
            public Stream<T> stream() {
                return Stream.of(value);
            }
        };
    }
}
