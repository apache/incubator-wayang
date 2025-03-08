package org.components.criterion;

import java.util.Map;
import java.util.function.Function;

public class Criterion {
    private final Function<Map<String, Object>, Boolean> shouldStop;

    public Criterion(Function<Map<String, Object>, Boolean> shouldStop) {
        this.shouldStop = shouldStop;
    }

    public boolean check(Map<String, Object> currentValues) {
        return shouldStop.apply(currentValues);
    }
}
