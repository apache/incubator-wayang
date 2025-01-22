package org.example;

import java.io.Serializable;

// Functional interface for single argument
@FunctionalInterface
public interface MonoFunction<T, S> extends Serializable {
    S apply(T a);
}
