package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.UnionAllOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Java implementation of the {@link UnionAllOperator}.
 */
public class JavaUnionAllOperator<Type>
        extends UnionAllOperator<Type>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param type the type of the datasets to be coalesced
     */
    public JavaUnionAllOperator(DataSetType<Type> type) {
        super(type);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        outputs[0].acceptStream(Stream.concat(inputs[0].provideStream(), inputs[1].provideStream()));
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(Configuration configuration) {
        return Optional.of(new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(2, 1, 0.9d, (inCards, outCards) -> 1000000),
                LoadEstimator.createFallback(2, 1)
        ));
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaUnionAllOperator<>(this.getInputType0());
    }
}
