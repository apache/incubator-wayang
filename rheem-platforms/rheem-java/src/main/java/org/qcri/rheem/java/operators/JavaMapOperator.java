package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Optional;
import java.util.stream.Stream;

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
        super(inputType, outputType, functionDescriptor);
    }

    @Override
    public Stream[] evaluate(Stream[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final Stream<InputType> inputStream = (Stream<InputType>) inputStreams[0];
        final Stream<OutputType> outputStream = inputStream.map(compiler.compile(this.functionDescriptor));

        return new Stream[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaMapOperator<>(getInputType(), getOutputType(), getFunctionDescriptor());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(Configuration configuration) {
        final NestableLoadProfileEstimator operatorEstimator = new NestableLoadProfileEstimator(
                DefaultLoadEstimator.createIOLinearEstimator(this, 10000),
                DefaultLoadEstimator.createIOLinearEstimator(this, 1000),
                DefaultLoadEstimator.createIOLinearEstimator(this, 1000),
                DefaultLoadEstimator.createIOLinearEstimator(this, 0)
        );
        final LoadProfileEstimator functionEstimator =
                configuration.getFunctionLoadProfileEstimatorProvider().provideFor(this.getFunctionDescriptor());
        operatorEstimator.nest(functionEstimator);

        return Optional.of(operatorEstimator);
    }
}
