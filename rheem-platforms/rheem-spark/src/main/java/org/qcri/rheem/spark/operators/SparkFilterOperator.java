package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.function.Predicate;

/**
 * Spark implementation of the {@link FilterOperator}.
 */
public class SparkFilterOperator<Type>
        extends FilterOperator<Type>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public SparkFilterOperator(DataSetType<Type> type, Predicate<Type> predicate) {
        super(type, predicate);
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final JavaRDD<Type> inputStream = (JavaRDD<Type>) inputStreams[0];

        final JavaRDD<Type> outputStream = inputStream.filter(item -> predicate.test(item));

        return new JavaRDDLike[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkFilterOperator<>(getInputType(), getFunctionDescriptor());
    }
}
