package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.basic.operators.DistinctOperator;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;


/**
 * Spark implementation of the {@link DistinctOperator}.
 */
public class SparkDistinctOperator<Type>
        extends DistinctOperator<Type>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public SparkDistinctOperator(DataSetType<Type> type) {
        super(type);
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputRdds, FunctionCompiler compiler) {
        if (inputRdds.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final JavaRDD<Type> inputStream = (JavaRDD<Type>) inputRdds[0];
        final JavaRDD<Type> outputStream = inputStream.distinct();

        return new JavaRDDLike[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkDistinctOperator<>(getInputType());
    }
}
