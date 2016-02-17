package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;


/**
 * Spark implementation of the {@link FlatMapOperator}.
 */
public class SparkFlatMapOperator<InputType, OutputType>
        extends FlatMapOperator<InputType, OutputType>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param functionDescriptor
     */
    public SparkFlatMapOperator(DataSetType inputType, DataSetType outputType,
                                FlatMapDescriptor<InputType, OutputType> functionDescriptor) {
        super(inputType, outputType, functionDescriptor);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final FlatMapFunction<InputType, OutputType> flatMapFunction =
                compiler.compile(this.functionDescriptor, this, inputs);

        final JavaRDD<InputType> inputRdd = inputs[0].<InputType>provideRdd();
        final JavaRDD<OutputType> outputRdd = inputRdd.flatMap(flatMapFunction);
        outputs[0].acceptRdd(outputRdd);
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkFlatMapOperator<>(this.getInputType(), this.getOutputType(), this.getFunctionDescriptor());
    }
}
