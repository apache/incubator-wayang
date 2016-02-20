package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.CartesianOperator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.OperatorBase;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Java implementation of the {@link CartesianOperator}.
 */
public class JavaCartesianOperator<InputType0, InputType1>
        extends CartesianOperator<InputType0, InputType1>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     */
    public JavaCartesianOperator(DataSetType<InputType0> inputType0, DataSetType<InputType1> inputType1) {
        super(inputType0, inputType1);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler) {
        if (inputs.length != 2) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        if (inputs[0].canProvideCollection()) {
            final Collection<InputType0> collection = inputs[0].provideCollection();
            final Stream<InputType1> stream = inputs[1].provideStream();
            outputs[0].acceptStream(stream.flatMap(e1 -> collection.stream().map(e0 -> new Tuple2<InputType0, InputType1>(e0, e1))));
        } else if (inputs[1].canProvideCollection()) {
            final Stream<InputType0> stream = inputs[0].provideStream();
            final Collection<InputType1> collection = inputs[1].provideCollection();
            outputs[0].acceptStream(stream.flatMap(e0 -> collection.stream().map(e1 -> new Tuple2<InputType0, InputType1>(e0, e1))));
        } else {
            // Fallback: Materialize one side.
            final Collection<InputType0> collection = (Collection<InputType0>) inputs[0].provideStream().collect(Collectors.toList());
            final Stream<InputType1> stream = inputs[1].provideStream();
            outputs[0].acceptStream(stream.flatMap(e1 -> collection.stream().map(e0 -> new Tuple2<InputType0, InputType1>(e0, e1))));
        }
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaCartesianOperator<>(this.getInputType0(), this.getInputType1());
    }
}
