package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.basic.operators.UnionAllOperator;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.stream.Stream;

/**
 * Spark implementation of the {@link UnionAllOperator}.
 */
public class SparkUnionAllOperator<Type>
        extends UnionAllOperator<Type>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     *
     */
    public SparkUnionAllOperator(DataSetType<Type> type) {
        super(type);
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputRdds, FunctionCompiler compiler) {
        if (inputRdds.length != 2) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final JavaRDD<Type> inputStream0 = (JavaRDD<Type>) inputRdds[0];
        final JavaRDD<Type> inputStream1 = (JavaRDD<Type>) inputRdds[1];
        final JavaRDD<Type> outputStream = inputStream0.union(inputStream1);

        return new JavaRDDLike[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkUnionAllOperator<>(getInputType0());
    }
}
