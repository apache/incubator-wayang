package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.CountOperator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Collections;
import java.util.stream.Stream;

/**
 * Java implementation of the {@link CountOperator}.
 */
public class JavaCountOperator<Type>
        extends CountOperator<Type>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public JavaCountOperator(DataSetType<Type> type) {
        super(type);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final long count = inputs[0].provideStream().count();
        outputs[0].acceptCollection(Collections.singleton(count));
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaCountOperator<>(this.getInputType());
    }
}
