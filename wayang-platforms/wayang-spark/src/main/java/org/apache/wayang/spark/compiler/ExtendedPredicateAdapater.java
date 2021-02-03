package org.apache.wayang.spark.compiler;

import org.apache.spark.api.java.function.Function;
import org.apache.wayang.core.function.ExecutionContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.spark.execution.SparkExecutionContext;

/**
 * Implements a {@link Function} that calls {@link org.apache.wayang.core.function.ExtendedFunction#open(ExecutionContext)}
 * of its implementation before delegating the very first {@link Function#call(Object)}.
 */
public class ExtendedPredicateAdapater<Type> implements Function<Type, Boolean> {

    private final PredicateDescriptor.ExtendedSerializablePredicate<Type> impl;

    private final SparkExecutionContext executionContext;

    private boolean isFirstRun = true;

    public ExtendedPredicateAdapater(PredicateDescriptor.ExtendedSerializablePredicate<Type> extendedFunction,
                                     SparkExecutionContext sparkExecutionContext) {
        this.impl = extendedFunction;
        this.executionContext = sparkExecutionContext;
    }

    @Override
    public Boolean call(Type dataQuantume) throws Exception {
        if (this.isFirstRun) {
            this.impl.open(this.executionContext);
        }

        return this.impl.test(dataQuantume);
    }

}
