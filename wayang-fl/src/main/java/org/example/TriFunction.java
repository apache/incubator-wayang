package org.example;

import java.io.Serializable;

// Functional interface for two arguments
@FunctionalInterface
public interface TriFunction<S, T, U> extends Serializable {
    S apply(T a, U b);
}
