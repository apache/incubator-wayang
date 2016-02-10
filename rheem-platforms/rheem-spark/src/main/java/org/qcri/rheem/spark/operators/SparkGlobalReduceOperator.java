package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.basic.operators.GlobalReduceOperator;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.Arrays;
import java.util.List;

/**
 * Spark implementation of the {@link GlobalReduceOperator}.
 */
public class SparkGlobalReduceOperator<Type>
        extends GlobalReduceOperator<Type>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public SparkGlobalReduceOperator(DataSetType<Type> type,
                                    ReduceDescriptor<Type> reduceDescriptor) {
        super(type, reduceDescriptor);
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputRdds, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        if (inputRdds.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final JavaRDD<Type> inputStream = (JavaRDD<Type>) inputRdds[0];

        final Type output = inputStream.reduce(compiler.compile(this.reduceDescriptor));
        List<Type> data = Arrays.asList(output);
        // FIXME: This is likely inefficient. The SparkExecutor should be capable of handling these primitive values and put them into RDDs on demand only. However, let's see if this is really a problem.
        JavaRDD<Type> outputStream = sparkExecutor.sc.parallelize(data);
        return new JavaRDDLike[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkGlobalReduceOperator<>(this.getInputType(), this.getReduceDescriptor());
    }
}
