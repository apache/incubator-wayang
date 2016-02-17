package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.DistinctOperator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.stream.Stream;

/**
 * Java implementation of the {@link DistinctOperator}.
 */
public class JavaDistinctOperator<Type>
        extends DistinctOperator<Type>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public JavaDistinctOperator(DataSetType<Type> type) {
        super(type);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        outputs[0].acceptStream(inputs[0].provideStream().distinct());
    }

    @Override
    public ExecutionOperator copy() {
        return new JavaDistinctOperator<>(this.getInputType());
    }
}
