package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.Optional;


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
        super(functionDescriptor, inputType, outputType);
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

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> 700 * inputCards[0] + 500000000L),
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> 10000),
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> 0),
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> Math.round(0.2 * outputCards[0] + 2000)),
                0.16d,
                420
        );

        return Optional.of(mainEstimator);
    }
}
