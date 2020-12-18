package io.rheem.rheem.spark.compiler;

import org.apache.spark.api.java.function.Function;
import io.rheem.rheem.core.function.ExecutionContext;
import io.rheem.rheem.spark.execution.SparkExecutionContext;

/**
 * Implements a {@link Function} that calls {@link io.rheem.rheem.core.function.ExtendedFunction#open(ExecutionContext)}
 * of its implementation before delegating the very first {@link Function#call(Object)}.
 */
public class ExtendedFunction<InputType, OutputType> implements Function<InputType, OutputType> {

    private final Function<InputType, OutputType> impl;

    private final SparkExecutionContext executionContext;

    private boolean isFirstRun = true;

    public <T extends Function<InputType, OutputType> & io.rheem.rheem.core.function.ExtendedFunction>
    ExtendedFunction(T extendedFunction,
                     SparkExecutionContext sparkExecutionContext) {
        this.impl = extendedFunction;
        this.executionContext = sparkExecutionContext;
    }

    @Override
    public OutputType call(InputType v1) throws Exception {
        if (this.isFirstRun) {
            ((io.rheem.rheem.core.function.ExtendedFunction) this.impl).open(this.executionContext);
            this.isFirstRun = false;
        }

        return this.impl.call(v1);
    }

}
