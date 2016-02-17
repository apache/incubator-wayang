package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.UnionAllOperator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

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
    public ExecutionOperator copy() {
        return new JavaUnionAllOperator<>(this.getInputType0());
    }
}
