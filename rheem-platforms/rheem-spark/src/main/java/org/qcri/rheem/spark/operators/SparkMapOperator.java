package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
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
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        // TODO: Correct open method.

        final JavaRDD<InputType> inputRdd = inputs[0].provideRdd();
        final Function<InputType, OutputType> mapFunctions = compiler.compile(this.functionDescriptor, this, inputs);
        final JavaRDD<OutputType> outputRdd = inputRdd.map(mapFunctions);

        outputs[0].acceptRdd(outputRdd);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkMapOperator<>(this.getInputType(), this.getOutputType(), this.getFunctionDescriptor());
    }
}
