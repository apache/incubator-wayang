package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.basic.operators.SortOperator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import scala.Tuple2;

/**
 * Spark implementation of the {@link SortOperator}.
 */
public class SparkSortOperator<Type>
        extends SortOperator<Type>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public SparkSortOperator(DataSetType<Type> type) {
        super(type);
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputRdds, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        if (inputRdds.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final JavaRDD<Type> inputStream = (JavaRDD<Type>) inputRdds[0];

        // TODO: Better sort function!
        final JavaRDD<Type> outputStream = inputStream.mapToPair(x->new Tuple2<Type,
                Boolean>(x, true)).sortByKey().map(y-> y._1);


        return new JavaRDDLike[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkSortOperator<>(getInputType());
    }

}

