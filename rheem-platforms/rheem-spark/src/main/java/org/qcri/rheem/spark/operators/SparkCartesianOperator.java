package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.basic.operators.CartesianOperator;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;


/**
 * Spark implementation of the {@link CartesianOperator}.
 */
public class SparkCartesianOperator<InputType0, InputType1>
        extends CartesianOperator<InputType0, InputType1>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     *
     */
    public SparkCartesianOperator(DataSetType<InputType0> inputType0, DataSetType<InputType1> inputType1) {
        super(inputType0, inputType1);
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 2) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }
        final JavaRDD<InputType0> inputStream0 = (JavaRDD<InputType0>) inputStreams[0];
        final JavaRDD<InputType1> inputStream1 = (JavaRDD<InputType1>) inputStreams[1];

        final JavaPairRDD<InputType0, InputType1> outputStream = inputStream0.cartesian(inputStream1);


        return new JavaRDDLike[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkCartesianOperator<>(getInputType0(), getInputType1());
    }
}
