package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/**
 * Java implementation of the {@link FlatMapOperator}.
 */
public class JavaFlatMapOperator<InputType, OutputType>
        extends FlatMapOperator<InputType, OutputType>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param functionDescriptor
     */
    public JavaFlatMapOperator(DataSetType inputType, DataSetType outputType,
                               FlatMapDescriptor<InputType, OutputType> functionDescriptor) {
        super(functionDescriptor, inputType, outputType);
    }

    @Override
    public void open(ChannelExecutor[] inputs, FunctionCompiler compiler) {
        final Function<InputType, Iterable<OutputType>> udf = compiler.compile(this.functionDescriptor);
        JavaExecutor.openFunction(this, udf, inputs);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Function<InputType, Iterable<OutputType>> flatmapFunction = compiler.compile(this.functionDescriptor);
        JavaExecutor.openFunction(this, flatmapFunction, inputs);

        outputs[0].acceptStream(
                inputs[0].<InputType>provideStream().flatMap(dataQuantum ->
                        StreamSupport.stream(
                                Spliterators.spliteratorUnknownSize(
                                        flatmapFunction.apply(dataQuantum).iterator(),
                                        Spliterator.ORDERED),
                                false
                        )
                )
        );
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaFlatMapOperator<>(this.getInputType(), this.getOutputType(), this.getFunctionDescriptor());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(Configuration configuration) {
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(this.getNumInputs(), 1, .8d, (inputCards, outputCards) -> 563 * inputCards[0] + 100511687),
                new DefaultLoadEstimator(this.getNumInputs(), 1, 0, (inputCards, outputCards) -> 0)
        );
        mainEstimator.nest(configuration.getFunctionLoadProfileEstimatorProvider().provideFor(this.getFunctionDescriptor()));
        return Optional.of(mainEstimator);
    }
}
