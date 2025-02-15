package org.functions;

import java.io.Serializable;

// Functional interface for two arguments
@FunctionalInterface
public interface PlanFunction<S, T, U> extends Serializable {
    S apply(T a, U b);
}