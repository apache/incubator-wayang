package org.example;

import java.io.Serializable;

// Functional interface for two arguments
@FunctionalInterface
public interface BiFunction<T, S> extends Serializable {
    S apply(T a, T b);
}
