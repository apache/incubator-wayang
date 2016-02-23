package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.operators.GlobalReduceOperator;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.Collections;
import java.util.Optional;
import java.util.function.BinaryOperator;

/**
 * Java implementation of the {@link GlobalReduceOperator}.
 */
public class JavaGlobalReduceOperator<Type>
        extends GlobalReduceOperator<Type>
        implements JavaExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type             type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public JavaGlobalReduceOperator(DataSetType<Type> type,
                                    ReduceDescriptor<Type> reduceDescriptor) {
        super(reduceDescriptor, type);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final BinaryOperator<Type> reduceFunction = compiler.compile(this.reduceDescriptor);
        JavaExecutor.openFunction(this, reduceFunction, inputs);

        final Optional<Type> reduction = inputs[0].<Type>provideStream().reduce(reduceFunction);
        outputs[0].acceptCollection(reduction.isPresent() ?
                Collections.singleton(reduction.get()) :
                Collections.emptyList());
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaGlobalReduceOperator<>(this.getInputType(), this.getReduceDescriptor());
    }
}
