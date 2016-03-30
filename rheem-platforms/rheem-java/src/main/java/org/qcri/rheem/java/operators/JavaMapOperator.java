package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Java implementation of the {@link org.qcri.rheem.basic.operators.MapOperator}.
 */
public class JavaMapOperator<InputType, OutputType>
        extends MapOperator<InputType, OutputType>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param functionDescriptor
     */
    public JavaMapOperator(DataSetType inputType, DataSetType outputType, TransformationDescriptor<InputType, OutputType> functionDescriptor) {
        super(functionDescriptor, inputType, outputType);
    }

    @Override
    public void open(ChannelExecutor[] inputs, FunctionCompiler compiler) {
        final Function<InputType, OutputType> udf = compiler.compile(this.functionDescriptor);
        JavaExecutor.openFunction(this, udf, inputs);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Function<InputType, OutputType> function = compiler.compile(this.functionDescriptor);
        JavaExecutor.openFunction(this, function, inputs);

        outputs[0].acceptStream(inputs[0].<InputType>provideStream().map(function));
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaMapOperator<>(this.getInputType(), this.getOutputType(), this.getFunctionDescriptor());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(Configuration configuration) {
        final NestableLoadProfileEstimator operatorEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> 55 * inputCards[0] + 380000),
                new DefaultLoadEstimator(1, 1, 0, (inputCards, outputCards) -> 0)
        );
        final LoadProfileEstimator functionEstimator =
                configuration.getFunctionLoadProfileEstimatorProvider().provideFor(this.getFunctionDescriptor());
        operatorEstimator.nest(functionEstimator);

        return Optional.of(operatorEstimator);
    }
}
