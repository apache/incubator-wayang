package org.qcri.rheem.spark.operators;

import org.apache.commons.lang3.Validate;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.basic.operators.StdoutSink;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

/**
 * Implementation of the {@link LocalCallbackSink} operator for the Java platform.
 */
public class SparkStdoutSink<T> extends StdoutSink<T> implements SparkExecutionOperator {
    /**
     * Creates a new instance.
     *
     * @param type     type of the incoming elements
     */
    public SparkStdoutSink(DataSetType<T> type) {
        super(type);
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputRdds, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        Validate.isTrue(inputRdds.length == 1);
        ((JavaRDDLike<T, ?>) inputRdds[0]).toLocalIterator().forEachRemaining(System.out::println);
        return new JavaRDDLike[0];
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkStdoutSink<>(this.getType());
    }
}
