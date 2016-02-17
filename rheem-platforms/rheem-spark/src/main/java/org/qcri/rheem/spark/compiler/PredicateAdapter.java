package org.qcri.rheem.spark.compiler;

import org.apache.spark.api.java.function.Function;

import java.util.function.Predicate;

/**
 * Wraps a {@link Predicate} as a {@link Function}.
 */
public class PredicateAdapter<InputType> implements Function<InputType, Boolean> {

    private Predicate<InputType> predicate;

    public PredicateAdapter(Predicate<InputType> predicate) {
        this.predicate = predicate;
    }

    @Override
    public Boolean call(InputType dataQuantum) throws Exception {
        return this.predicate.test(dataQuantum);
    }
}
