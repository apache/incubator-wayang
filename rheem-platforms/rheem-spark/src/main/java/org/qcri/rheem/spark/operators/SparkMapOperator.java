package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;


/**
 * Spark implementation of the {@link org.qcri.rheem.basic.operators.MapOperator}.
 */
public class SparkMapOperator<InputType, OutputType>
        extends MapOperator<InputType, OutputType>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param functionDescriptor
     */
    public SparkMapOperator(DataSetType inputType, DataSetType outputType,
                            TransformationDescriptor<InputType, OutputType> functionDescriptor) {
        super(inputType, outputType, functionDescriptor);
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputRdds, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        if (inputRdds.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final JavaRDD<InputType> inputStream = (JavaRDD<InputType>) inputRdds[0];
        final JavaRDD<OutputType> outputStream = inputStream.map(compiler.compile(this.functionDescriptor));

        return new JavaRDDLike[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkMapOperator<>(this.getInputType(), this.getOutputType(), this.getFunctionDescriptor());
    }
}
