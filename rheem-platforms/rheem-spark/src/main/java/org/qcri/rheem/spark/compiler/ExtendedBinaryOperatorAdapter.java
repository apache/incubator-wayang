package org.qcri.rheem.spark.compiler;

import org.apache.spark.api.java.function.Function2;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.spark.execution.SparkExecutionContext;

/**
 * Implements a {@link Function2} that calls {@link org.qcri.rheem.core.function.ExtendedFunction#open(ExecutionContext)}
 * of its implementation before delegating the very first {@link Function2#call(Object, Object)}.
 */
public class ExtendedBinaryOperatorAdapter<Type> implements Function2<Type, Type, Type> {

    private final FunctionDescriptor.ExtendedSerializableBinaryOperator<Type> impl;

    private final SparkExecutionContext executionContext;

    private boolean isFirstRun = true;

    public ExtendedBinaryOperatorAdapter(FunctionDescriptor.ExtendedSerializableBinaryOperator<Type> extendedFunction,
                                         SparkExecutionContext sparkExecutionContext) {
        this.impl = extendedFunction;
        this.executionContext = sparkExecutionContext;
    }

    @Override
    public Type call(Type dataQuantum0, Type dataQuantum1) throws Exception {
        if (this.isFirstRun) {
            this.impl.open(this.executionContext);
            this.isFirstRun = false;
        }

        return this.impl.apply(dataQuantum0, dataQuantum1);
    }

}
