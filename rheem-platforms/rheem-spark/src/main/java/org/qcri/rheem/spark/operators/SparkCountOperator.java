package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.operators.CountOperator;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.Arrays;
import java.util.List;


/**
 * Spark implementation of the {@link CountOperator}.
 */
public class SparkCountOperator<Type>
        extends CountOperator<Type>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public SparkCountOperator(DataSetType<Type> type) {
        super(type);
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputRdds, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        if (inputRdds.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final JavaRDD<Type> inputStream = (JavaRDD<Type>) inputRdds[0];
        final Long count = inputStream.count();

        List<Long> data = Arrays.asList(count);
        JavaRDD<Long> distData = sparkExecutor.sc.parallelize(data);

        return new JavaRDDLike[]{distData};
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkCountOperator<>(getInputType());
    }
}
